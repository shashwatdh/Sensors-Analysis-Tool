import paho.mqtt.client as mqtt
import ssl
from time import sleep
from random import randint
import json
import datetime
import os
import numpy as np
import pandas as pd
import sys
"""
    Sensor Node:
    - In main thread data will be generated and pushed to a list(queue).
    - Callback function will be executed in sep thread and can push data on data channel.
    - On successful connection or receiving ACK, disbale the lock for producer to publish
      data on channel.
    - producer on appending the data to queue checks if its allowed to publish head data
      packet.
"""

# Configuring the client node
sensors_config = os.path.join("..", "sensors_config.json")
with open(sensors_config) as fp:
    config_data = json.load(fp)
fp.close()

sensor_config = config_data["sensors"][1]
sensor_id = sensor_config["id"]
data_path = sensor_config["dataset_path"] 
window_size = sensor_config['window_size']
step_size = window_size * sensor_config["step_size"]
reqd_cols = sensor_config["dataset_reqd_cols"]

#connflag = False
publish_data_lock = True
sub_topic = "2/ack" # Topic to receive ACK
data_list = []

datatset_rows_limit = 109

#dataset = pd.read_csv(data_path, header=None)
dataset = pd.read_csv(data_path)
X = dataset.iloc[:datatset_rows_limit, reqd_cols].values

base_TS = (X[0,0] // window_size) * window_size
#base_TS += step_size if (base_TS + step_size) <= X[0,0] else 0

#base_TS = X[0,0]
sensor_data = []
window_data = {
                "sensor_id" : sensor_id,
                "base_Timestamp" : 0,
                "data" : []
                }

def push_data_pck():
    data_pck = data_list.pop(0) # Remove head packet
    mqttc.publish("sensors/data", json.dumps(data_pck), qos=1)

def push_to_dataQ(sensor_data):
    global publish_data_lock
    global base_TS
    
    # Pushes sensor data to data queue
    window_data["base_Timestamp"] = base_TS
    window_data["data"] = sensor_data
    data_list.append(window_data.copy())
    
    # check if producer can publish data
    if not publish_data_lock:
        push_data_pck()
        publish_data_lock = True


# Setting up connection with data channel
def on_connect(client, userdata, flags, rc):                        
    global publish_data_lock                                                 
    print("Connection to AWS")
    #connflag = True
    print("Connection returned result: " + str(rc) )
    # Subscribe to ACK topic
    mqttc.subscribe(sub_topic, 1)
    
    if len(data_list) and (data_list[-1]['data'] == "End"):  
        push_data_pck()
        
    publish_data_lock = False           # Enable producer to publish data on channel 

def on_message(mosq, obj, msg):
    global publish_data_lock
    print("Topic: " + str(msg.topic))
    print("ACK: " + str(msg.payload))
    
    """
        Data ACK is rcvd. 
        
        If END packet has already been pushed to queue, then
        from data queue push head packet on data channel.
        
        Else, just disbale the lock for producer to push 
        data on channel.
    """
    res = json.loads(msg.payload)
    print(res)
    if res["status"] == 403:
        # 
        print("Disconnecting.....")
        mqttc.unsubscribe(sub_topic)
        mqttc.loop_stop()    #Stop loop 
        mqttc.disconnect() # disconnect
        sys.exit(1)
    else:
        print(res["status"])
        
    if len(data_list) and (data_list[-1]['data'] == "End"):  
        push_data_pck()
    
    else:
        publish_data_lock = False
    """
    def push_data_pck():
        data_pck = data_list.pop(0) # Remove head packet
        mqttc.publish("sensors/data", json.dumps(data_pck), qos=1)
    
    # Check if there is some data to send
    if len(data_list):  
        push_data_pck()
    """
        
def on_subscribe(mosq, obj, mid, granted_qos):
    print("Subscribed to Topic: " + sub_topic)

def on_publish(client,userdata,result):             #create function for callback
    print("data published: \n")
    print(result)

# Initiate MQTT Client
mqttc = mqtt.Client("sensor_gyro")

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe
mqttc.on_publish = on_publish 

#### Change following parameters #### 
awshost = "a2jdem77nz5dot-ats.iot.us-east-1.amazonaws.com"                  # endpoint
awsport = 8883   
caPath = os.path.join(".", "certs_gyro","AmazonRootCA1(2).pem")
certPath = os.path.join(".", "certs_gyro","6741f706b58e3bf8c139bee31df1ee0c74b862cae56866f1f981a4f2d51661b9-certificate.pem.crt.txt")
keyPath = os.path.join(".", "certs_gyro","6741f706b58e3bf8c139bee31df1ee0c74b862cae56866f1f981a4f2d51661b9-private.pem.key")                                                   # port no.   

mqttc.tls_set(ca_certs=caPath, certfile=certPath, keyfile=keyPath, cert_reqs=ssl.CERT_REQUIRED,
              tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)       # pass parameters
 
mqttc.connect(awshost, awsport, keepalive=60)                       # connect to AWS server
mqttc.loop_start()                                                 # start background network thread
                                         
    
for rec in X[:]:
    
    # storing values recorded in a time frame into a list
    if rec[0] < (base_TS + window_size):
        sensor_data.append(rec[:].tolist()) 
    
    else:
        
        '''
        Record belongs to different window. So first append populated data
        to data queue.
        
        Set base_TS value and clear window_data. Add new entry in 
        window_data.
        '''
        
        if len(sensor_data):
            push_to_dataQ(sensor_data[:])
        
        base_TS += step_size
        # from cur window remove entries having timestamp < updated base_TS
        while len(sensor_data) and sensor_data[0][0] < base_TS:
            del sensor_data[0]
        #sensor_data = [rec for rec in sensor_data if rec[0] >= base_TS]         
        # Check if row can be added in slided window
        if rec[0] >= (base_TS + window_size):
            """ 
                cur rec can't be added to window, so push cur sensor data to
                data queue. And set cur rec's TS as base TS
            
            """
            if len(sensor_data):
                push_to_dataQ(sensor_data[:])
            
            sensor_data = []
            """
                base_TS can't be assigned to some random value. We must calculate
                base_TS of window nearest to rec if no error would have occured. 
            """
            base_TS = (rec[0] // window_size) * window_size
            #base_TS += step_size if (base_TS + step_size) <= rec[0] else 0
        
        sensor_data.append(rec[:].tolist())
        
# Push last data packets to queue
while base_TS <= rec[0]:
    push_to_dataQ(sensor_data[:])
    base_TS += step_size
    # reove entries having baseTS < updated baseTS
    while len(sensor_data) and sensor_data[0][0] < base_TS:
        del sensor_data[0]
    #sensor_data = [rec_i for rec_i in sensor_data if rec_i[0] >= base_TS]
        
# Push END packet
push_to_dataQ("End")
