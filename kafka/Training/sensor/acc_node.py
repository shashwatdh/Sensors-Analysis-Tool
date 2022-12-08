from pickle import FALSE
import time
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
from kafka import KafkaConsumer
from kafka import KafkaProducer
import threading

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
sensors_config = os.path.join(".", "1155FF54-63D3-4AB2-9863-8385D0BD0A13.features_labels", "1155FF54-63D3-4AB2-9863-8385D0BD0A13.features_labels.csv")
"""with open(sensors_config) as fp:
    config_data = json.load(fp)
fp.close()"""

reqdCols = [0,18,19,20,-1]

timestampCol= 0
accXCol = 18
accYCol = 19
accZCol = 20
labelCol = -1



window_size = 120
step_size = window_size * 0.5

dataset = pd.read_csv(sensors_config)
X = dataset.iloc[:2000,reqdCols].values

base_TS = (X[0,0] // window_size) * window_size

sensor_id = "1"
sub_topic = "1_ack"
publish_data_lock = False # default - False

sensor_data = []
data_list = []
window_data = {
                "sensor_id" : sensor_id,
                "base_Timestamp" : 0,
                "data" : []
                }

window_data_train = {
                "sensor_id" : sensor_id,
                "base_Timestamp" : 0,
                "label": 0,
                "data" : []
                }


'''
sensor_config = config_data["sensors"][0]
sensor_id = sensor_config["id"]
data_path = sensor_config["dataset_path"] 
window_size = sensor_config['window_size']
step_size = window_size * sensor_config["step_size"]
reqd_cols = sensor_config["dataset_reqd_cols"]'''

"""
#connflag = False
publish_data_lock = False
sub_topic = "1_ack" # Topic to receive ACK
pub_topic = "sensors_data"
data_list = []

"""
pub_topic = "sensors_data"

def push_data_pck():
    data_pck = data_list.pop(0) # Remove head packet
    #mqttc.publish("sensors/data", json.dumps(data_pck), qos=1)
    producer.send(pub_topic, json.dumps(data_pck).encode("utf-8"))
    print("Pushed the window data")

def push_to_dataQ(sensor_data):
    global publish_data_lock
    # Pushes sensor data to data queue
    window_data_train["base_Timestamp"] = base_TS
    #window_data_train["data"] = sensor_data
    if sensor_data != "End":
        window_data_train["label"] = sensor_data[0][-1]
        window_data_train["data"] = np.array(sensor_data[:])[:,:-1].tolist()
    else:
        window_data_train["label"] = -1
    data_list.append(window_data_train.copy())
    print("Data pushed:", window_data_train.copy())
    
    # check if producer can publish data
    
    if not publish_data_lock:
        push_data_pck()
        publish_data_lock = True


consumer = KafkaConsumer(sub_topic, bootstrap_servers='localhost:9092')
producer = KafkaProducer(bootstrap_servers="localhost:9092")
abort_proc = False
thread_started = False

def bck_thread():
    #global lock
    global producer
    global abort_proc
    global thread_started
    global data_list
    global consumer
    global publish_data_lock

    #consumer = KafkaConsumer(sub_topic, bootstrap_servers='localhost:9092')
    print("acc_thread started....")
    thread_started = True

    for msg in consumer:
               
        r_msg = json.loads(msg.value.decode("utf-8"))
        #print(json.loads(msg)["order_id"])
        print(r_msg["status"])
        if r_msg["status"] == 403:
            abort_proc = True
            data_list = []
            break
        
        else:
            print("rcvd ack - ", r_msg["status"])
            if len(data_list):
                print("packet BS (to be sent):", data_list[0]["base_Timestamp"])
                push_data_pck()
                #data_pck = data_list.pop(0) # Remove head packet
                #producer.send("order_details", data_pck) #json.dumps(data_pck).encode("utf-8"))
                print("packet sent...")
            else:
                publish_data_lock = False
                print("No Data Found, released the lock")


t1 = threading.Thread(target=bck_thread)
t1.start()

while not thread_started:
    continue

time.sleep(10)
print("Start segmenting windows....")



for i in range(len(X[:])):

    if abort_proc:
        break
    
    # storing values recorded in a time frame into a list
    if X[i][0] < (base_TS + window_size) and (i==0 or (i>0 and X[i][-1] == X[i-1][-1])):
        sensor_data.append(X[i][:].tolist()) 
    
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
        if X[i][0] >= (base_TS + window_size) or (len(sensor_data) and X[i][-1] != sensor_data[0][-1]):
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
            base_TS = (X[i][0] // window_size) * window_size
            #base_TS += step_size if (base_TS + step_size) <= rec[0] else 0
        
        sensor_data.append(X[i][:].tolist())
        
# Push last data packets to queue
while base_TS <= X[i][0]:
    push_to_dataQ(sensor_data[:])
    base_TS += step_size
    # reove entries having baseTS < updated baseTS
    while len(sensor_data) and sensor_data[0][0] < base_TS:
        del sensor_data[0]




"""
for rec in X:
    
    if abort_proc:
        break

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
            print("pushing win to data list")
            push_to_dataQ(sensor_data[:])
        
        base_TS += step_size
        # from cur window remove entries having timestamp < updated base_TS
        while len(sensor_data) and sensor_data[0][0] < base_TS:
            del sensor_data[0]
        #sensor_data = [rec for rec in sensor_data if rec[0] >= base_TS]         
        # Check if row can be added in slided window
        if rec[0] >= (base_TS + window_size):
             
            #    cur rec can't be added to window, so push cur sensor data to
            #    data queue. And set cur rec's TS as base TS
            
            
            if len(sensor_data):
                print("pushing win to data list")
                push_to_dataQ(sensor_data[:])
            
            sensor_data = []
            
            #    base_TS can't be assigned to some random value. We must calculate
            #    base_TS of window nearest to rec if no error would have occured. 
            
            base_TS = (rec[0] // window_size) * window_size
            #base_TS += step_size if (base_TS + step_size) <= rec[0] else 0
        
        sensor_data.append(rec[:].tolist())
        
# Push last data packets to queue
while not abort_proc and base_TS <= rec[0]:
    push_to_dataQ(sensor_data[:])
    base_TS += step_size
    # remove entries having baseTS < updated baseTS
    while len(sensor_data) and sensor_data[0][0] < base_TS:
        del sensor_data[0]
    #sensor_data = [rec_i for rec_i in sensor_data if rec_i[0] >= base_TS]
"""
        
# Push END packet
if not abort_proc:
    push_to_dataQ("End")

t1.join()