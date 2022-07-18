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
from feat_extr_module import *
import time
from kafka import KafkaConsumer
from kafka import KafkaProducer

"""
    Initial Setup:
        1) Read Config file
        2) Extract cols for which feat needs to be extracted
        3) Validate if cols exist
        4) Extarct feat
"""

# 1) Read sensors config file
sensors_config = os.path.join("..", "sensors_config.json")
with open(sensors_config) as fp:
    config_data = json.load(fp)
fp.close()

feat_ext_config = config_data["feature_extraction"]
feat_ext_preprocessing_config = config_data["feature_extraction"]["pre_processing"]
feat_ext_types = list(config_data["feature_extraction"]["types"].keys())

file_name = "feat_ext_" + fetch_time_stmp() + ".csv"
file_path = os.path.join(".", file_name)
file_rec_buf = []
recs_buf_limit = feat_ext_config["file_handling"]["rec_buf_limit"]
rec_counter = 0
# 2) Extract cols and create header for file
"""
header = []
for feat_ext_typ in feat_ext_config:
    for feat in feat_ext_typ:"""


#MQTT_PORT = 8883
#MQTT_KEEPALIVE_INTERVAL = 45

subTopic = "merge_node_feat_ext"
consumer = KafkaConsumer(subTopic, bootstrap_servers='localhost:9092')

#AWS_ENDPOINT = "a2jdem77nz5dot-ats.iot.us-east-1.amazonaws.com"
#CA_PATH = os.path.join(".", "certs_feat_ext","AmazonRootCA1(3).pem")
#CERT_PATH = os.path.join(".", "certs_feat_ext","65fe1c57c917cc21c3e80564098ef8daf0a97b6aed117764b5d6907e87f61a41-certificate.pem.crt.txt")
#KEY_PATH = os.path.join(".", "certs_feat_ext","65fe1c57c917cc21c3e80564098ef8daf0a97b6aed117764b5d6907e87f61a41-private.pem.key")                                                   # port no.   


abort_proc = False
"""
def send_ack(sensor_id, status):
    # Sends ack
    res_data = {"status" : status}
    print(res_data)
    mqttc.publish(publish_topics[sensor_id], json.dumps(res_data), qos=1)
"""
def file_dump():
    global rec_counter
    global file_rec_buf
    feat_extr_df = pd.DataFrame(file_rec_buf)
    feat_extr_df.to_csv(file_path, mode="a", header=False)
    rec_counter = 0
    file_rec_buf = []
    
def process_data_packet(payload):
    #pass        
    """
    features = []
    # For each type of feat_extr, pass its config along with data
    for feat_ext_type in feat_ext_types:
        #for feat in list(feat_ext_config[feat_ext_type].keys()):
         if feat_ext_type == "statistical":   
            features.extend(extract_statistical_feat(payload, feat_ext_config[feat_ext_type]))"""
    global rec_counter
    global file_rec_buf
    global abort_proc
    #print(payload)
    #merged_data = pd.DataFrame(payload)
    rcvd_merged_data = json.loads(payload)
    print("len of data rcvd:",len(rcvd_merged_data["data"]))
    
    if len(rcvd_merged_data["data"]) == 0:#rcvd_merged_data["data"] == "end":
        print("Aborting the feature extraction module")
        abort_proc = True
        return 
        """
        if rec_counter: #len(file_rec_buf):
            file_dump()
        mqttc.unsubscribe(MQTT_TOPIC)
        mqttc.loop_stop()    #Stop loop 
        mqttc.disconnect() # disconnect
        sys.exit(0)"""
    else:
        print("Normal exec")
        
    merged_data = rcvd_merged_data["data"][:]
    merged_df = pd.DataFrame(merged_data, columns=rcvd_merged_data["header"])
    
    # Perform Preprocessing steps
    # Handle missing data values
    
    # for now remove rows with missing data
    handle_missing_rows = feat_ext_preprocessing_config["data_cleaning"]["missing_data"]
    if handle_missing_rows == "discard":
        merged_df.dropna(axis=0, how="any", inplace=True)
    else:
        pass
    
    # Remove redundant entries
    # normalization or standarisation
    
    # feature extraction
    features = [rcvd_merged_data["data"][0][0]]
    header = [rcvd_merged_data["header"][0]]
    for feat_ext_type in feat_ext_types:
        #for feat in list(feat_ext_config[feat_ext_type].keys()):
         if feat_ext_type == "statistical":   
            #features.extend(extract_statistical_feat(merged_df, feat_ext_config["types"][feat_ext_type]))
            extr_features, extr_header = extract_statistical_feat(merged_df, feat_ext_config["types"][feat_ext_type])
            features.extend(extr_features)
            if not exists(file_path):
                header.extend(extr_header)
    
    if not exists(file_path):
        pd.DataFrame([header]).to_csv(file_path, mode="a", header=False)
        
    print("feat extr:",features)
    file_rec_buf.append(features[:])
    rec_counter += 1
    if rec_counter == recs_buf_limit:
        # push recs to file
        file_dump()
        #feat_extr_df = pd.DataFrame([file_rec_buf])
        #feat_extr_df.to_csv(file_path, mode="a", header=False)
        #rec_counter = 0
        #file_rec_buf = []
    
    #print("Extr features:",features)
    """
    if not exists(file_path):
        pd.DataFrame([header]).to_csv(file_path, mode="a", header=False)
        #feat_extr_df.to_csv(file_path, mode="a", header=True)
    else:
        feat_extr_df.to_csv(file_path, mode="a", header=False)
    """
    #extract_statistical_feat(merged_df, )
            
# Define on connect event function
# We shall subscribe to our Topic in this function
"""def on_connect(mosq, obj, flags,rc):
    mqttc.subscribe(MQTT_TOPIC, 1)"""

# Define on_message event function. 
# This function will be invoked every time,
# a new message arrives for the subscribed topic 
for message in consumer:
    #msg = json.loads(message.value.decode("utf-8"))
    process_data_packet(message.value.decode("utf-8"))
    #print(json.loads(msg)["order_id"])
    if abort_proc:
        if rec_counter: #len(file_rec_buf):
            file_dump()
        break
"""
def on_message(mosq, obj, msg):
    global abort_proc
    print("Topic: " + str(msg.topic))
    print("QoS: " + str(msg.qos))
    print("Payload: " + str(msg.payload))
    # Process received data packet
    process_data_packet(msg.payload)
    if abort_proc:
        if rec_counter: #len(file_rec_buf):
            file_dump()
        #mqttc.unsubscribe(MQTT_TOPIC)
        #mqttc.loop_stop()    #Stop loop 
        #mqttc.disconnect() # disconnect
        sys.exit(0)
"""    
'''    
def on_subscribe(mosq, obj, mid, granted_qos):
    print("Subscribed to Topic: " + MQTT_TOPIC)

def on_publish(client,userdata,result):             #create function for callback
    print("data published: \n")
    print(result)
'''
'''
# Initiate MQTT Client
mqttc = mqtt.Client()

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe
mqttc.on_publish = on_publish
# Configure TLS Set
mqttc.tls_set(ca_certs=CA_PATH, certfile=CERT_PATH, keyfile=KEY_PATH, cert_reqs=ssl.CERT_REQUIRED,
              tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None) 

# Connect with MQTT Broker
mqttc.connect(AWS_ENDPOINT, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)

# Continue monitoring the incoming messages for subscribed topic
mqttc.loop_forever()
'''
    