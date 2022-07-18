import time
import pandas as pd
import numpy as np
import os
import json
from inertial_sensor import * 
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

def initialize_inertial_sensor(config):
    #obj = sensor(config_data)
    #obj.sense_send('acc2_data.csv', conn)
    obj = inertial_sensors(config)
    while not obj.inertial_sensor.thread_started: #thread_started:
        continue

    time.sleep(10)
    obj.send()

if __name__ == "__main__":
    
    
    sensors_config = os.path.join("..", "sensors_config.json")#("..", "..", "sensors_config.json")
    with open(sensors_config) as fp:
        config_data = json.load(fp)
    fp.close()

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