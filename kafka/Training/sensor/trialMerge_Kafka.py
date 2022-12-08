import paho.mqtt.client as mqtt
import ssl
import os
import json
import numpy as np
import pandas as pd
from functools import reduce
import sys
from os.path import exists


from services import fetch_time_stmp, df_cols
from missingDataImputer import missing_data_impute_exp
from feat_extr_module import extract_statistical_feat
import time
from kafka import KafkaConsumer
from kafka import KafkaProducer

"""
    Initial Setup:
        1) For each sensor initailize the empty state.
            - Fetch sensors related information from config file and initialize empty state for each sensor
            - Set value of counting semaphore equal to no. of sensors
            
        2) Configure the connection with data channel.
            - Set topic for each sensor to send ACK
            - Subscribe to a topic to receive data from all sensors.
"""

# 1) Initailizing the state

# 1.1) Read sensors config file
sensors_config = os.path.join(".", "sensors_config.json")
with open(sensors_config) as fp:
    config_data = json.load(fp)
fp.close()

file_name = "sensors_merged_data_test_" + fetch_time_stmp() + ".csv"
sensor_data = config_data["sensors"]
merge_config = config_data["merge"]
handle_missing_win = merge_config["handle_missing_window"]
forcedTermination = False

sensors_state = {}
last_commit_baseTS = -1   
publish_topics = {}
merge_data = {}
sensors_end_req = [] # List of sensors requested to end the trans
sensors_id = []
sensors_end_count = 0
tot_sensor_nodes = len(sensor_data) 


# Initializing empty state for each sensor
for sensor in sensor_data:
    sensors_state[sensor["id"]] = {}
    sensors_state[sensor["id"]]["window_size"] = sensor["window_size"]
    sensors_state[sensor["id"]]["step_size"] = sensor["step_size"]
    sensors_state[sensor["id"]]["base_Timestamp"] = -1
    sensors_state[sensor["id"]]["rec_base_Timestamp"] = -1
    sensors_state[sensor["id"]]["data"] = []
    #  Initialize merge_data for each sensor
    merge_data[sensor["id"]] = {"base_Timestamp" : -1,
                                "data" : [],
                                "label" : 0, #new
                                "forced_merge" : False}#[]
    # Initialize column headers
    merge_data[sensor["id"]]["cols"] = df_cols(sensor["id"], len(sensor["dataset_reqd_cols"]))
    print(sensor["id"], merge_data[sensor["id"]]["cols"])
    # 2.1) Setting topic for each sensor to publish ACK
    publish_topics[sensor["id"]] = str(sensor["id"]) + "_ack"
    sensors_id.append(sensor["id"]) #unnecessary, use sensor_data instead

print(merge_data)
#print(merge_data[1]["cols"])
# 1.2) Initializing value of counting semaphore

counting_semaphore = len(sensor_data)
subTopic = "sensors_data"
featExtr_pub_topic = "merge_node_feat_ext"

consumer = KafkaConsumer(subTopic, bootstrap_servers='localhost:9092')
producer = KafkaProducer(bootstrap_servers="localhost:9092")

Abort_Proc = False
def send_ack(sensor_id, status):
    # Sends ack
    res_data = {"status" : status}
    print(res_data)
    producer.send(publish_topics[sensor_id], json.dumps(res_data).encode("utf-8"))
    #mqttc.publish(publish_topics[sensor_id], json.dumps(res_data), qos=1)


def process_data_packet(payload):
    """
        Processes received data packet.
        Tasks performed:
            1) check if rcvd packet is valid.
                - packet is invalid if packets's baseTS < last_commit_TS
            2) append data to a sensor's state.
            3) decrement counting semaphore's val if a sensor has sent packet for 
                the first time after last commit.
            4) pushing sensors' head data to merge_data, after counting semaphore
                value is set to 0 and apply merge().
            5) set appropriate value of counting semaphore and last_commit_TS,
                after data is merged.
                - val of semaphore is set to no. of sensors having len(data[]) = 0(empty data).
        
        On recv END packet,
            1) set merge_data for corresponding sensor to NULL
            2) remove sensor state of sensor
    """
    global counting_semaphore
    global last_commit_baseTS
    global file_name
    global Abort_Proc
    global forcedTermination
    global sensors_end_count
    forcedTermination = False
    isSensorDataListEmpty = False
    allSensorsStateSynced = False

    rcvd_data = json.loads(payload)
    sensor_id = rcvd_data["sensor_id"]
    if Abort_Proc:
        return
    # check if rcvd packet is END packet
    if rcvd_data["data"] == "End" or rcvd_data["label"] < 0:
        
        if len(sensors_state[sensor_id]["data"]) == 0:
                    
            for s_id in sensors_id:
                # clear state
                merge_data[s_id]["data"] = []
                del sensors_state[s_id]
                #del publish_topics[sensor]
                #sensors_end_count += 1
                    
                # send ack to sensor node to disconnect
                send_ack(s_id, 403)
                
            print("Aborting Merge Operation......")
            Abort_Proc = True
            data = json.dumps({"header":"", "data":[]}) 
            producer.send(featExtr_pub_topic, data.encode("utf-8"))
            print("Sent END signal to fet_extr module:", data)
            return
            # Once state of all sensors have been cleared, unsubscribe to MQTT
            # topic, disconnect from broker and terminate the process.
           
        else:
            sensors_end_req.append(sensor_id)
        return 
    
    # Check for validity of data packet
    if rcvd_data["base_Timestamp"] < last_commit_baseTS:
        # invalid data, as old data is rcvd. But send ACK
        send_ack(sensor_id, 200)
    else:
        # valid data
        """
            Check if new data has been rcvd from a sensor and decr semaphore val.
            Otherwise just append the data.
        """
        if sensor_id not in sensors_state:
            
            sys.exit(0)
        print("rcvd data from sensor - ", sensor_id, rcvd_data)
        if len(sensors_state[sensor_id]["data"]) == 0:
            counting_semaphore -= 1
            print(counting_semaphore)
            # Setting base timestamp of sensor, as its the first packet after merge
            sensors_state[sensor_id]["base_Timestamp"] = rcvd_data["base_Timestamp"]
        
        # Append data
        new_data = {"base_Timestamp" : rcvd_data["base_Timestamp"],
                    "data" : rcvd_data["data"],
                    "label": rcvd_data["label"] #new
                    }
        sensors_state[sensor_id]["data"].append(new_data)
        # Update rec_baseTS
        sensors_state[sensor_id]["rec_base_Timestamp"] = rcvd_data["base_Timestamp"]
        # Send ACK 
        send_ack(sensor_id, 200)
        # Check if semaphore val == 0
        if counting_semaphore == 0:
            """
                1) Recent data packet caused semaphore val = 0, so its base_TS
                must be set as last_TS ( as well as last_commit_TS) and all 
                data packets till this timestamp must be merged. But it's possible
                that other sensors may not have yet rcvd the packet. Thst's we must 
                try to compare last packet of each sensor and select last_TS as oldest 
                (smallest) base_ts.
                
                2) The first data packet of each sensor must be compared to find
                smallest(oldest) baseTS : start_TS, from where we can start 
                merging the data.
                
                3) While start_TS <= last_TS, check if head packet of sensors can
                be merged. check if base_ts - start_ts <= threshold. If cond is 
                satisfied head data packet will be pushed for merging else null 
                will be pushed.
                
                4) After merging the data, repeat step-2.
            """
            def fetch_lastTS_sensor():
                # Fetch oldest recent_timestamp 
                max_TS = rcvd_data["base_Timestamp"]
                for sensor in sensors_id:#sensors_state.keys():
                    TS = sensors_state[sensor]["rec_base_Timestamp"] # or sensors_state[sensor]["data"][0]["base_Timestamp"] 
                    if (TS >= 0) and (TS < max_TS):
                        max_TS = TS
                return max_TS
            
            last_TS = fetch_lastTS_sensor()#rcvd_data["base_Timestamp"]
            
            def fetch_startTS_sensor():
                #nonlocal sensor_id
                # Fetch nearest timestamp from last_commit_TS and corresponding sensor
                minBT_sensor = sensor_id
                min_TS = last_TS + (sensors_state[sensor_id]["window_size"] * sensors_state[sensor_id]["step_size"]) # lastTS + step_size, even +1 wud have worked
                for sensor in sensors_id:#sensors_state.keys():
                    TS = sensors_state[sensor]["base_Timestamp"] # or sensors_state[sensor]["data"][0]["base_Timestamp"] 
                    if (TS >= 0) and (TS < min_TS):
                        (min_TS, minBT_sensor) = (TS, sensor)
                return (min_TS, minBT_sensor)
                
            (start_TS, sensor) = fetch_startTS_sensor()
            if start_TS < 0:  #optional
                return
            while start_TS <= last_TS and not forcedTermination:
                threshold = sensors_state[sensor]["window_size"] * sensors_state[sensor]["step_size"] # threshold = step_size  
                reqLabel = sensors_state[sensor]["data"][0]["label"]
                # Check if head packet of a sensor can be merged
                for sensor in sensors_id:#sensors_state.keys():
                    if not len(sensors_state[sensor]["data"]):
                        forcedTermination = True
                        sensors_state[sensor]["base_Timestamp"] = -1
                        break

                    head_packet = sensors_state[sensor]["data"][0]
                    #if head_packet["label"] != reqLabel:
                    #    print("Dataset Label Mismatch")


                    if head_packet["base_Timestamp"] - start_TS < threshold and head_packet["label"] == reqLabel:
                        # push sensor data to merge_data, delete head packet and
                        # update sensor's baseTS
                        #merge_data[sensor] = head_packet#["data"]  #**************pd.DataFrame(data)
                        merge_data[sensor]["base_Timestamp"] = head_packet["base_Timestamp"]
                        merge_data[sensor]["label"] = head_packet["label"]
                        merge_data[sensor]["data"] = head_packet["data"]
                        merge_data[sensor]["forced_merge"] = False

                        
                        del sensors_state[sensor]["data"][0]
                        if len(sensors_state[sensor]["data"]):
                            sensors_state[sensor]["base_Timestamp"] = sensors_state[sensor]["data"][0]["base_Timestamp"]
                        else:
                            sensors_state[sensor]["base_Timestamp"] = -1 # all data packets have been merged, this can also be used to check if semaphore val can be dec 
                            #forcedTermination = True
                            isSensorDataListEmpty = True
                    else:
                        print("Unable to sync windows with same label")
                        start_TS = head_packet["base_Timestamp"]
                        reqLabel = head_packet["label"]
                        threshold = sensors_state[sensor]["window_size"] * sensors_state[sensor]["step_size"]
                        #allSensorsStateSynced = False
                        ################# Important##############################

                        while not allSensorsStateSynced:
                            for sensorId in sensors_id:
                                if not len(sensors_state[sensorId]["data"]):
                                    allSensorsStateSynced = True
                                    sensors_state[sensor]["base_Timestamp"] = -1
                                    forcedTermination = True #imp
                                    # clear merge data and stop merge module
                                    print("No data packet found", sensorId)
                                    break
                                head_data_packet = sensors_state[sensorId]["data"][0]
                                if head_data_packet["label"] != reqLabel or (abs(head_data_packet["base_Timestamp"] - start_TS) >= threshold):
                                    '''
                                    if len(merge_data[sensorId]["data"]):#optional as anyway it will be overwritten with correct params
                                        merge_data[sensorId] = {"base_Timestamp" : -1,
                                                                "data" : [],
                                                                "label" : 0, #new
                                                                "forced_merge" : False,
                                                                "cols": df_cols(sensor["id"], len(sensor["dataset_reqd_cols"]))
                                                                }'''

                                    if head_data_packet["base_Timestamp"] > start_TS:
                                        start_TS = head_data_packet["base_Timestamp"] 
                                        reqLabel = head_data_packet["label"]
                                        threshold = sensors_state[sensorId]["window_size"] * sensors_state[sensorId]["step_size"]
                                        allSensorsStateSynced = False
                                        continue
                                    del sensors_state[sensorId]["data"][0]
                                else:
                                    #merge_data[sensorId]["base_Timestamp"] = head_data_packet["base_Timestamp"]
                                    #merge_data[sensorId]["label"] = head_data_packet["label"]
                                    #merge_data[sensorId]["data"] = head_data_packet["data"]
                                    #merge_data[sensorId]["forced_merge"] = False
                                    allSensorsStateSynced = True
                        break
                        #######################################################################    
                            #if forcedTermination:
                            #    allSensorsStateSynced = True
                            #    for sensorId in sensors_id:

                        # push [] to merge_data as head packet can't be merged
                        #merge_data[sensor]["data"] = []
                        # Forcefully merge the data, merged window can have imputed vals
                        # or can be discarded
                        #merge_data[sensor]["forced_merge"] = True Important/required
                # merge the valid head data packets
                def merge_data_pck():
                    cols = ["timestamp", "x","y","z"]
                    tmp_cols = ["timestamp"]
                    dataframes = []
                    
                    
                    """
                        Merging and imputation.
                            
                        For each sensor check if forced_merge option is set. If it is, then we 
                        need to impute values for missing window and save as dataframe. Else,
                        just store data as dataframe and perform merge.
                        
                    """
                    
                    left = sensors_id[0]#sensors[0]
                    print("debug-",left)
                    #print("left:",left,merge_data[left])
                    left_cols = merge_data[left]["cols"]
                    label = merge_data[left]["label"]
                    
                    #print("left_cols:",left_cols)
                    if merge_data[left]["forced_merge"]:
                        if handle_missing_win == "discard":
                            return
                        elif handle_missing_win == "impute":
                            last_pkt = merge_data[left]
                            next_pkt = ({"base_Timestamp":-1, "data":[]}, 
                                            sensors_state[left]["data"][0]) [len(sensors_state[left]["data"]) != 0]
                            left_df = missing_data_impute_exp(last_pkt, next_pkt, start_TS, left_cols)
                            print("Imputed Left_df:",left_df)
                            #print(left_df)
                            if left_df is None:
                                return
                    else:
                        #print("left_df data:", merge_data[left])#["data"])
                        left_df = pd.DataFrame(merge_data[left]["data"], columns = left_cols)
                        #print("imputed left_df:", left_df)
                        #print("left cols:", left_cols)
                    
                    right = sensors_id[1]#sensors[1]
                    right_cols = merge_data[right]["cols"]
                    if merge_data[right]["forced_merge"]:
                        if handle_missing_win == "discard":
                            return
                        elif handle_missing_win == "impute":
                            last_pkt = merge_data[right]
                            next_pkt = ({"base_Timestamp":-1, "data":[]}, 
                                            sensors_state[right]["data"][0]) [len(sensors_state[right]["data"]) != 0]
                            right_df = missing_data_impute_exp(last_pkt, next_pkt, start_TS, right_cols)
                            print("Imputed rt_df:",right_df)
                            #print(right_df)
                            if right_df is None:
                                return
                    else:
                        right_df = pd.DataFrame(merge_data[right]["data"], columns = right_cols)
                    
                    
                    if merge_data[left]["forced_merge"] and not merge_data[right]["forced_merge"]:
                        # swap DFs order as it's better to use actual data as left_index for merging
                        print("Before merge, left:",left_df)
                        
                        left_df = pd.merge_asof(right_df, left_df, on = "timestamp",tolerance = 2.0, direction = "backward")
                        rearranged_cols = left_cols + right_cols[1:]
                        print("rearranged cols:",rearranged_cols)
                        print("left_df",left_df)
                        left_df = left_df.loc[:, rearranged_cols]
                    
                    else:
                        left_df = pd.merge_asof(left_df, right_df, on = "timestamp",tolerance = 2.0, direction = "backward")
                        
                    for sensor in sensors_id[2:]:#sensors[2:]:
                        cols =  merge_data[sensor]["cols"]
                        if merge_data[sensor]["forced_merge"]:
                            if handle_missing_win == "discard":
                                return
                            elif handle_missing_win == "impute":
                                last_pkt = merge_data[sensor]
                                next_pkt = ({"base_Timestamp":-1, "data":[]}, 
                                                sensors_state[sensor]["data"][0]) [len(sensors_state[sensor]["data"]) != 0]
                                right_df = missing_data_impute_exp(last_pkt, next_pkt, start_TS, cols)
                                if right_df is None:
                                    return
                        else:
                            right_df = pd.DataFrame(merge_data[sensor]["data"], columns = cols)
                        
                        left_df = pd.merge_asof(left_df, right_df, on = "timestamp",tolerance = 2.0, direction = "backward")
                      
                    file_path = os.path.join(".", file_name)
                    if not exists(file_path):    
                        left_df.to_csv(file_path, mode="a", header=True)
                    else:
                        left_df.to_csv(file_path, mode="a")
                    print(left_df)
                    print("file:", file_path)
                    #extract_statistical_feat(left_df)
                    print(left_df.columns.tolist())
                    merged_data = json.dumps({"header":left_df.columns.tolist(),"data":left_df.values.tolist(), "label":label}) 
                    #data = json.dumps({"header":"", "data":[]}) 
                    producer.send(featExtr_pub_topic, merged_data.encode("utf-8"))
                    print("Sent data to fet_extr module:", merged_data)
                    #---------------------------
                    #mqttc.publish("merge_node/feat_ext", merged_data, qos=1) #IMPORTANT
                    #---------------------------
                #------------VIMP---------------
                """
                if not forcedTermination or not allSensorsStateSynced:
                    merge_data_pck()
                    print("merged some data pkts")
                    if isSensorDataListEmpty:
                        forcedTermination = True
                        continue
                    last_commit_baseTS = start_TS
                    (start_TS, sensor) = fetch_startTS_sensor()"""
                #-----------------------------------'
                if forcedTermination:
                    continue
                if not allSensorsStateSynced:
                    merge_data_pck()
                    print("merged some data pkts")
                    if isSensorDataListEmpty:
                        forcedTermination = True
                        continue
                        #return
                last_commit_baseTS = start_TS
                (start_TS, sensor) = fetch_startTS_sensor()
            
            
            """
            if forcedTermination:
                # following code snippet can be defined as func
                for s_id in sensors_id:
                    # clear state
                    merge_data[s_id]["data"] = []
                    del sensors_state[s_id]
                    #del publish_topics[sensor]
                    sensors_end_count += 1
                        
                    # send ack to sensor node to disconnect
                    send_ack(s_id, 403)
                            
                    print("Aborting Merge Operation......")
                    # Once state of all sensors have been cleared, unsubscribe to MQTT
                    # topic, disconnect from broker and terminate the process.
                    Abort_Proc = True
                    data = json.dumps({"header":"", "data":[]}) 
                    producer.send(featExtr_pub_topic, data.encode("utf-8"))
                    print("Sent END signal to fet_extr module:", data)
                    return
            """
            # handle end req
            if len(sensors_end_req):
                print("rcvd end req", sensors_end_req)
                """ 
                    some sensor has put end_trans req
                    Before clearing state, its imp to check if no data is stored and 
                    old data has been merged.
                    
                    If data field is set in sensors_state that means there is data yet
                    to be merged and we can't clear state.
                    
                    If END pkt has been received from any sensor, abort merge operaton
                    and clear state of all the sensorss.
                """
                
                #global sensors_end_count
                for sensor in sensors_end_req:
                    #if (sensor in sensors_state) and len(sensors_state[sensor]["data"]) == 0:
                    if len(sensors_state[sensor]["data"]) == 0:
                        
                        for s_id in sensors_id:
                            # clear state
                            merge_data[s_id]["data"] = []
                            del sensors_state[s_id]
                            #del publish_topics[sensor]
                            sensors_end_count += 1
                        
                            # send ack to sensor node to disconnect
                            send_ack(s_id, 403)
                            
                        print("Aborting Merge Operation......")
                        # Once state of all sensors have been cleared, unsubscribe to MQTT
                        # topic, disconnect from broker and terminate the process.
                        Abort_Proc = True
                        data = json.dumps({"header":"", "data":[]}) 
                        producer.send(featExtr_pub_topic, data.encode("utf-8"))
                        print("Sent END signal to fet_extr module:", data)
                        return 
                       
                
            
            """
                Once reqd data packets have been merged, set the value of semaphore equal to 
                no. of sensors having base_ts < 0 or len(data[]) = 0
            """
            
            for sensor in sensors_state.keys():
                if sensors_state[sensor]["base_Timestamp"] < 0:
                    counting_semaphore += 1
    print("@@@@@@@@@@@@@@@@End of pkt handler@@@@@@@@@@@@@@@@@@")
        
for message in consumer:
    #msg = json.loads(message.value.decode("utf-8"))
    process_data_packet(message.value.decode("utf-8"))
    #print(json.loads(msg)["order_id"])
    if Abort_Proc:
        break

