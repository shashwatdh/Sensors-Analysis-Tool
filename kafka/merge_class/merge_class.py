# -*- coding: utf-8 -*-
"""
Created on Sun May  8 16:04:59 2022

@author: shash
"""

import paho.mqtt.client as mqtt
import ssl
import os
import json
import numpy as np
import pandas as pd
from functools import reduce
import sys
from os.path import exists
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
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

class merge:
    def __init__(self, merge_config_path):
        print("Hello")
        # 1) Initailizing the state

        # 1.1) Read sensors config file
        #self.sensors_config = os.path.join("..", "sensors_config.json")
        #with open(sensors_config) as fp:
        with open(merge_config_path) as fp:
            self.config_data = json.load(fp)
        fp.close()

        self.file_name = "sensors_merged_data_test_" + fetch_time_stmp() + ".csv"
        self.sensor_data = self.config_data["sensors"]
        self.merge_config = self.config_data["merge"]
        self.handle_missing_win = self.merge_config["handle_missing_window"]

        self.comm_channel = self.merge_config["comm_channel"] #config_data["comm_channel"]
        self.Abort_Proc = False
        
        self.sensors_state = {}
        self.last_commit_baseTS = -1   
        self.publish_topics = {}
        self.merge_data = {}
        self.sensors_end_req = [] # List of sensors requested to end the trans
        self.sensors_id = []
        self.sensors_end_count = 0
        self.tot_sensor_nodes = len(self.sensor_data) 

        # Initializing empty state for each sensor
        for sensor in self.sensor_data:
            self.sensors_state[sensor["id"]] = {}
            self.sensors_state[sensor["id"]]["window_size"] = sensor["window_size"]
            self.sensors_state[sensor["id"]]["step_size"] = sensor["step_size"]
            self.sensors_state[sensor["id"]]["base_Timestamp"] = -1
            self.sensors_state[sensor["id"]]["rec_base_Timestamp"] = -1
            self.sensors_state[sensor["id"]]["data"] = []
            #  Initialize merge_data for each sensor
            self.merge_data[sensor["id"]] = {"base_Timestamp" : -1,
                                            "data" : [],
                                            "forced_merge" : False}#[]
            # Initialize column headers
            self.merge_data[sensor["id"]]["cols"] = df_cols(sensor["id"], len(sensor["dataset_reqd_cols"]))
            # 2.1) Setting topic for each sensor to publish ACK
            if self.comm_channel == "AWS_IoT" :
                self.publish_topics[sensor["id"]] = str(sensor["id"]) + "/ack"
            else:
                self.publish_topics[sensor["id"]] = str(sensor["id"]) + "_ack"
            self.sensors_id.append(sensor["id"]) #unnecessary, use sensor_data instead

        print(self.merge_data)
            #print(merge_data[1]["cols"])
            # 1.2) Initializing value of counting semaphore
        self.counting_semaphore = len(self.sensor_data)

        #self.comm_channel = self.merge_config["comm_channel"] #config_data["comm_channel"]
        #self.Abort_Proc = False

        # 2.2) Setting connection with data channel
        if self.comm_channel == "AWS_IoT" :
            self.MQTT_PORT = 8883
            self.MQTT_KEEPALIVE_INTERVAL = 45
            self.MQTT_TOPIC = "sensors/data"

            self.AWS_ENDPOINT = "a2jdem77nz5dot-ats.iot.us-east-1.amazonaws.com"
            self.CA_PATH = os.path.join("..", "certs_merge","AmazonRootCA1(3).pem")
            self.CERT_PATH = os.path.join("..", "certs_merge","d056be5e3680c2fe06a1410774e479c33a3462e9e407c548f956031578c86e19-certificate.pem.crt.txt")
            self.KEY_PATH = os.path.join("..", "certs_merge","d056be5e3680c2fe06a1410774e479c33a3462e9e407c548f956031578c86e19-private.pem.key")                                                   # port no.   
            self.__connect2broker()

        elif self.comm_channel == "kafka":
            self.subTopic = "sensors_data"
            self.featExtr_pub_topic = "merge_node_feat_ext"

            self.consumer = KafkaConsumer(self.subTopic, bootstrap_servers='localhost:9092')
            self.producer = KafkaProducer(bootstrap_servers="localhost:9092")
            #self.msgHandler()

        

    def msgHandler(self):
        for message in self.consumer:
            #msg = json.loads(message.value.decode("utf-8"))
            self.process_data_packet(message.value.decode("utf-8"))
            #print(json.loads(msg)["order_id"])
            if self.Abort_Proc:
                break       

    def __connect2broker(self):
        # Initiate MQTT Client
        self.mqttc = mqtt.Client()

        # Assign event callbacks
        self.mqttc.on_message = self.on_message
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_subscribe = self.on_subscribe
        self.mqttc.on_publish = self.on_publish
        # Configure TLS Set
        self.mqttc.tls_set(ca_certs=self.CA_PATH, certfile=self.CERT_PATH, keyfile=self.KEY_PATH, cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None) 

        # Connect with MQTT Broker
        self.mqttc.connect(self.AWS_ENDPOINT, self.MQTT_PORT, self.MQTT_KEEPALIVE_INTERVAL)

        # Continue monitoring the incoming messages for subscribed topic
        self.mqttc.loop_forever()

    def send_ack(self, sensor_id, status):
        # Sends ack
        res_data = {"status" : status}
        print(res_data)
        if self.comm_channel == "kafka":
            print(self.publish_topics[sensor_id])
            self.producer.send(self.publish_topics[sensor_id], json.dumps(res_data).encode("utf-8"))
        else:
            self.mqttc.publish(self.publish_topics[sensor_id], json.dumps(res_data), qos=1)


    def process_data_packet(self, payload):
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
        #global counting_semaphore
        #global last_commit_baseTS
        #global file_name
        #global Abort_Proc
        rcvd_data = json.loads(payload)
        sensor_id = rcvd_data["sensor_id"]
            
        if self.Abort_Proc:
            return
        
        # check if rcvd packet is END packet
        if rcvd_data["data"] == "End":
                
            if len(self.sensors_state[sensor_id]["data"]) == 0:
                    
                for s_id in self.sensors_id:
                    # clear state
                    self.merge_data[s_id]["data"] = []
                    del self.sensors_state[s_id]
                    #del publish_topics[sensor]
                    #sensors_end_count += 1
                    
                    # send ack to sensor node to disconnect
                    self.send_ack(s_id, 403)
                
                print("Aborting Merge Operation......")
                self.Abort_Proc = True
                if self.comm_channel == "kafka":
                    data = json.dumps({"header":"", "data":[]}) 
                    self.producer.send(self.featExtr_pub_topic, data.encode("utf-8"))
                print("Sent END signal to fet_extr module:", data)
                return
                # Once state of all sensors have been cleared, unsubscribe to MQTT
                # topic, disconnect from broker and terminate the process.
                """
                data = json.dumps({"header":"", "data":[]}) 
                mqttc.publish("merge_node/feat_ext", data, qos=1)
                time.sleep(80)
                mqttc.unsubscribe(MQTT_TOPIC)
                mqttc.loop_stop()    #Stop loop 
                mqttc.disconnect() # disconnect
                sys.exit(0)"""
            else:
                self.sensors_end_req.append(sensor_id)
            return 

        # Check for validity of data packet
        if rcvd_data["base_Timestamp"] < self.last_commit_baseTS:
            # invalid data, as old data is rcvd. But send ACK
            self.send_ack(sensor_id, 200)
        
        else:
            # valid data
            """
                Check if new data has been rcvd from a sensor and decr semaphore val.
                Otherwise just append the data.
            """
            if sensor_id not in self.sensors_state:
            
                sys.exit(0)
        
            if len(self.sensors_state[sensor_id]["data"]) == 0:
                self.counting_semaphore -= 1
                # Setting base timestamp of sensor, as its the first packet after merge
                self.sensors_state[sensor_id]["base_Timestamp"] = rcvd_data["base_Timestamp"]
        
            # Append data
            new_data = {"base_Timestamp" : rcvd_data["base_Timestamp"],
                        "data" : rcvd_data["data"]}
            self.sensors_state[sensor_id]["data"].append(new_data)
            # Update rec_baseTS
            self.sensors_state[sensor_id]["rec_base_Timestamp"] = rcvd_data["base_Timestamp"]
            # Send ACK 
            print("rcvd data pkt frm:", sensor_id)
            print("rcvd data:", new_data)
            self.send_ack(sensor_id, 200)
            # Check if semaphore val == 0
            if self.counting_semaphore == 0:
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
                    for sensor in self.sensors_id:#sensors_state.keys():
                        TS = self.sensors_state[sensor]["rec_base_Timestamp"] # or sensors_state[sensor]["data"][0]["base_Timestamp"] 
                        if (TS >= 0) and (TS < max_TS):
                            max_TS = TS
                    return max_TS
            
                last_TS = fetch_lastTS_sensor()#rcvd_data["base_Timestamp"]
            
                def fetch_startTS_sensor():
                    nonlocal sensor_id
                    # Fetch nearest timestamp from last_commit_TS and corresponding sensor
                    minBT_sensor = sensor_id
                    min_TS = last_TS + (self.sensors_state[sensor_id]["window_size"] * self.sensors_state[sensor_id]["step_size"]) # lastTS + step_size, even +1 wud have worked
                    for sensor in self.sensors_id:#sensors_state.keys():
                        TS = self.sensors_state[sensor]["base_Timestamp"] # or sensors_state[sensor]["data"][0]["base_Timestamp"] 
                        if (TS >= 0) and (TS < min_TS):
                            (min_TS, minBT_sensor) = (TS, sensor)
                    return (min_TS, minBT_sensor)
                
                (start_TS, sensor) = fetch_startTS_sensor()
                while start_TS <= last_TS:
                    threshold = self.sensors_state[sensor]["window_size"] * self.sensors_state[sensor]["step_size"] # threshold = step_size  
                    # Check if head packet of a sensor can be merged
                    for sensor in self.sensors_id:#sensors_state.keys():
                        head_packet = self.sensors_state[sensor]["data"][0]
                        if head_packet["base_Timestamp"] - start_TS < threshold:
                            # push sensor data to merge_data, delete head packet and
                            # update sensor's baseTS
                            #merge_data[sensor] = head_packet#["data"]  #**************pd.DataFrame(data)
                            self.merge_data[sensor]["base_Timestamp"] = head_packet["base_Timestamp"]
                            self.merge_data[sensor]["data"] = head_packet["data"]
                            self.merge_data[sensor]["forced_merge"] = False
                        
                            del self.sensors_state[sensor]["data"][0]
                            if len(self.sensors_state[sensor]["data"]):
                                self.sensors_state[sensor]["base_Timestamp"] = self.sensors_state[sensor]["data"][0]["base_Timestamp"]
                            else:
                                self.sensors_state[sensor]["base_Timestamp"] = -1 # all data packets have been merged, this can also be used to check if semaphore val can be dec 
                        else:
                            # push [] to merge_data as head packet can't be merged
                            #merge_data[sensor]["data"] = []
                            # Forcefully merge the data, merged window can have imputed vals
                            # or can be discarded
                            self.merge_data[sensor]["forced_merge"] = True
                    
                    # merge the valid head data packets
                    def merge_data_pck():
                        cols = ["timestamp", "x","y","z"]
                        tmp_cols = ["timestamp"]
                        dataframes = []
                        #sensors = list(sensors_state.keys())
                        # refer https://stackoverflow.com/questions/44327999/python-pandas-merge-multiple-dataframes
                        """
                        def df_cols(sensor_id):
                            # returns dataframe columns
                            tot_cols = len(merge_data[sensor_id]["data"][0])
                            cols = ["timestamp"]
                            for i in range(1, tot_cols):
                                cols.append(str(sensor_id) + "_" + str(i))
                            return cols
                        """
                    
                        """
                            Merging and imputation.
                            
                            For each sensor check if forced_merge option is set. If it is, then we 
                            need to impute values for missing window and save as dataframe. Else,
                            just store data as dataframe and perform merge.
                        
                        """
                    
                        left = self.sensors_id[0]#sensors[0]
                        #print("left:",left,merge_data[left])
                        left_cols = self.merge_data[left]["cols"]
                    
                        #print("left_cols:",left_cols)
                        if self.merge_data[left]["forced_merge"]:
                            if self.handle_missing_win == "discard":
                                return
                            elif self.handle_missing_win == "impute":
                                last_pkt = self.merge_data[left]
                                next_pkt = ({"base_Timestamp":-1, "data":[]}, 
                                                self.sensors_state[left]["data"][0]) [len(self.sensors_state[left]["data"]) != 0]
                                left_df = missing_data_impute_exp(last_pkt, next_pkt, start_TS, left_cols)
                                print("Imputed Left_df:",left_df)
                                #print(left_df)
                                if left_df is None:
                                    return
                        else:
                            #print("left_df data:", merge_data[left])#["data"])
                            left_df = pd.DataFrame(self.merge_data[left]["data"], columns = left_cols)
                            #print("imputed left_df:", left_df)
                            #print("left cols:", left_cols)
                    
                        right = self.sensors_id[1]#sensors[1]
                        right_cols = self.merge_data[right]["cols"]
                    
                        if self.merge_data[right]["forced_merge"]:
                            if self.handle_missing_win == "discard":
                                return
                            elif self.handle_missing_win == "impute":
                                last_pkt = self.merge_data[right]
                                next_pkt = ({"base_Timestamp":-1, "data":[]}, 
                                            self.sensors_state[right]["data"][0]) [len(self.sensors_state[right]["data"]) != 0]
                                right_df = missing_data_impute_exp(last_pkt, next_pkt, start_TS, right_cols)
                                print("Imputed rt_df:",right_df)
                                #print(right_df)
                                if right_df is None:
                                    return
                        else:
                            right_df = pd.DataFrame(self.merge_data[right]["data"], columns = right_cols)
                    
                    
                        if self.merge_data[left]["forced_merge"] and not self.merge_data[right]["forced_merge"]:
                            # swap DFs order as it's better to use actual data as left_index for merging
                            print("Before merge, left:",left_df)
                        
                            left_df = pd.merge_asof(right_df, left_df, on = "timestamp",tolerance = 2.0, direction = "backward")
                            rearranged_cols = left_cols + right_cols[1:]
                            print("rearranged cols:",rearranged_cols)
                            print("left_df",left_df)
                            left_df = left_df.loc[:, rearranged_cols]
                    
                        else:
                            left_df = pd.merge_asof(left_df, right_df, on = "timestamp",tolerance = 2.0, direction = "backward")
                        
                        for sensor in self.sensors_id[2:]:#sensors[2:]:
                            cols =  self.merge_data[sensor]["cols"]
                            if self.merge_data[sensor]["forced_merge"]:
                                if self.handle_missing_win == "discard":
                                    return
                                elif self.handle_missing_win == "impute":
                                    last_pkt = self.merge_data[sensor]
                                    next_pkt = ({"base_Timestamp":-1, "data":[]}, 
                                                self.sensors_state[sensor]["data"][0]) [len(self.sensors_state[sensor]["data"]) != 0]
                                    right_df = missing_data_impute_exp(last_pkt, next_pkt, start_TS, cols)
                                    if right_df is None:
                                        return
                            else:
                                right_df = pd.DataFrame(self.merge_data[sensor]["data"], columns = cols)
                        
                            left_df = pd.merge_asof(left_df, right_df, on = "timestamp",tolerance = 2.0, direction = "backward")
                      
                        file_path = os.path.join(".", self.file_name)
                        if not exists(file_path):    
                            left_df.to_csv(file_path, mode="a", header=True)
                        else:
                            left_df.to_csv(file_path, mode="a")
                        print(left_df)
                        #extract_statistical_feat(left_df)
                        print(left_df.columns.tolist())
                        merged_data = json.dumps({"header":left_df.columns.tolist(),"data":left_df.values.tolist()}) 
                        if self.comm_channel == "kafka":
                            self.producer.send(self.featExtr_pub_topic, merged_data.encode("utf-8"))
                            print("Sent data to fet_extr module:", merged_data)
                        else:
                            self.mqttc.publish("merge_node/feat_ext", merged_data, qos=1)
                    merge_data_pck()
                    #global sensors_end_req
                    # Check for end_trans req
                     
                    self.last_commit_baseTS = start_TS
                    (start_TS, sensor) = fetch_startTS_sensor()
            
            
                # handle end req
                if len(self.sensors_end_req):
                    print("rcvd end req")
                    """ 
                        some sensor has put end_trans req
                        Before clearing state, its imp to check if no data is stored and 
                        old data has been merged.
                    
                        If data field is set in sensors_state that means there is data yet
                        to be merged and we can't clear state.
                    
                        If END pkt has been received from any sensor, abort merge operaton
                        and clear state of all the sensorss.
                    """
                
                    global sensors_end_count
                    for sensor in self.sensors_end_req:
                        #if (sensor in sensors_state) and len(sensors_state[sensor]["data"]) == 0:
                        if len(self.sensors_state[sensor]["data"]) == 0:
                        
                            for s_id in self.sensors_id:
                                # clear state
                                self.merge_data[s_id]["data"] = []
                                del self.sensors_state[s_id]
                                #del publish_topics[sensor]
                                self.sensors_end_count += 1
                        
                                # send ack to sensor node to disconnect
                                self.send_ack(s_id, 403)
                            
                            print("Aborting Merge Operation......")
                            # Once state of all sensors have been cleared, unsubscribe to MQTT
                            # topic, disconnect from broker and terminate the process.
                            self.Abort_Proc = True
                            if self.comm_channel == "kafka":
                                data = json.dumps({"header":"", "data":[]}) 
                                self.producer.send(self.featExtr_pub_topic, data.encode("utf-8"))
                                print("Sent END signal to fet_extr module:", data)
                         
                            return 
                            """
                            data = json.dumps({"header":"", "data":[]}) 
                            mqttc.publish("merge_node/feat_ext", data, qos=1)
                            time.sleep(80)
                            mqttc.unsubscribe(MQTT_TOPIC)
                            mqttc.loop_stop()    #Stop loop 
                            mqttc.disconnect() # disconnect
                            sys.exit(0)"""
                
            
                """
                    Once reqd data packets have been merged, set the value of semaphore equal to 
                    no. of sensors having base_ts < 0 or len(data[]) = 0
                """
            
                for sensor in self.sensors_state.keys():
                    if self.sensors_state[sensor]["base_Timestamp"] < 0:
                        self.counting_semaphore += 1
        
        
    # Define on connect event function
    # We shall subscribe to our Topic in this function
    def on_connect(self, mosq, obj, flags,rc):
        self.mqttc.subscribe(self.MQTT_TOPIC, 1)

    # Define on_message event function. 
    # This function will be invoked every time,
    # a new message arrives for the subscribed topic 
    def on_message(self, mosq, obj, msg):
        #global Abort_Proc
        print("Topic: " + str(msg.topic))
        print("QoS: " + str(msg.qos))
        print("Payload: " + str(msg.payload))
        # Process received data packet
        self.process_data_packet(msg.payload)
        if self.Abort_Proc == True:
            data = json.dumps({"header":"", "data":[]}) 
            self.mqttc.publish("merge_node/feat_ext", data, qos=1)
            #time.sleep(80)
            self.mqttc.unsubscribe(self.MQTT_TOPIC)
            self.mqttc.loop_stop()    #Stop loop 
            self.mqttc.disconnect() # disconnect
            sys.exit(0)
    
    def on_subscribe(self, mosq, obj, mid, granted_qos):
        print("Subscribed to Topic: " + self.MQTT_TOPIC)

    def on_publish(self,client,userdata,result):             #create function for callback
        print("data published: \n")
        print(result)
    