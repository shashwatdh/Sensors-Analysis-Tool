import time
import pandas as pd
import numpy as np
import os
import json
#from  sensors_class.inertial_sensor import * #import *
#from sensors_class import inertial_sensor, sensor
#import merge_class.merge_class
#from merge_class import *
from feature_extr import feat_extr_class
from sensors_class import inertial_sensor
from merge_class import merge_class
import multiprocessing
'''
sensors_config = os.path.join("..", "..", "sensors_config.json")
with open(sensors_config) as fp:
    config_data = json.load(fp)
fp.close()

sensor_config = config_data["sensors"][0]

sensor_acc = sensor(sensor_config)
while (not sensor_acc.dataset_read_completed) or (len(sensor_acc.data_windows_list)):
    data = sensor_acc.fetch_cur_window_and_slide()
    if not len(data):
        continue
    print("fetched Data:\n", data)
    sensor_acc.push_to_dataQ((data))

sensor_acc.push_to_dataQ({"base_Timestamp" : -1,
"data" : "End"})
print("abt 2 disconnect")
while(not sensor_acc.disconnected):
    continue

print("Disconnected")'''


# --------------------------------------------------------------------------
"""
sensors_config = os.path.join("..", "..", "sensors_config.json")
with open(sensors_config) as fp:
    config_data = json.load(fp)
fp.close()

sensors_initialized = []

for sensor_config in config_data["sensors"]:
    if sensor_config["type"] == "inertial": 
        sensors_initialized.append(inertial_sensors(sensor_config))
#acc = inertial_sensors(1)
#gyro = inertial_sensors(2)

print("Sending data")
for sensor in sensors_initialized:
    sensor.send()
#acc.send()
#gyro.send()

print("Disconnecting...")"""

def initialize_feat_extr_node(config_path):
    obj = feat_extr_class.feature_extraction(config_path)
    obj.msgHandler()


def initialize_merge_node(config_path):
    obj = merge_class.merge(config_path)
    obj.msgHandler()

def initialize_inertial_sensor(config):
    #obj = sensor(config_data)
    #obj.sense_send('acc2_data.csv', conn)
    obj = inertial_sensor.inertial_sensors(config)
    #obj.send()

    while not obj.inertial_sensor.thread_started: #thread_started:
        continue

    time.sleep(10)
    obj.send()

if __name__ == "__main__":
    
    
    sensors_config = os.path.join(".", "sensors_config.json")
    with open(sensors_config) as fp:
        config_data = json.load(fp)
    fp.close()

    feat_extr_proc = multiprocessing.Process(target=initialize_feat_extr_node, args=(sensors_config,))
    feat_extr_proc.start()
    

    merge_proc = multiprocessing.Process(target=initialize_merge_node, args=(sensors_config,))
    merge_proc.start()
    
    sensors_initialized = []

    for sensor_config in config_data["sensors"]:
        if sensor_config["type"] == "inertial": 
            #sensors_initialized.append(inertial_sensors(sensor_config))
            sensors_initialized.append(multiprocessing.Process(target=initialize_inertial_sensor, 
                                                                   args=(sensor_config,)))
    
    
    for sensor in sensors_initialized:
        sensor.start()
    
    for sensor in sensors_initialized:
        sensor.join()
    
    merge_proc.join()
    feat_extr_proc.join()
    #acc = inertial_sensors(1)
    #gyro = inertial_sensors(2)

    #print("Sending data")
    #for sensor in sensors_initialized:
    #    sensor.send()
    
    """
    # read sensor config data
    with open("./acc_config.json") as fp:
        config_data = json.load(fp)
    fp.close()
    obj = sensor(config_data)
    
    # creating a pipe
    master_conn, slave_conn = multiprocessing.Pipe()
  
    # creating new processes
    p1 = multiprocessing.Process(target=Master, args=(master_conn,))
    p2 = multiprocessing.Process(target=Slave, args=(slave_conn,))
  
    # running processes
    p1.start()
    p2.start()
  
    # wait until processes finish
    p1.join()
    p2.join()
    """