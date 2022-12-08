import time
import pandas as pd
import numpy as np
import os
import json
from inertial_sensor import * 
import multiprocessing


def initialize_inertial_sensor(config):
    obj = inertial_sensors(config)
    while not obj.inertial_sensor.thread_started: #thread_started:
        continue

    time.sleep(10)
    obj.send()

if __name__ == "__main__":
    
    
    sensors_config = os.path.join(".", "sensors_config.json")
    with open(sensors_config) as fp:
        config_data = json.load(fp)
    fp.close()

    sensors_initialized = []

    for sensor_config in config_data["sensors"]:
        if sensor_config["type"] == "inertial": 
            sensors_initialized.append(multiprocessing.Process(target=initialize_inertial_sensor, 
                                                                   args=(sensor_config,)))
    
    
    for sensor in sensors_initialized:
        sensor.start()
    
    for sensor in sensors_initialized:
        sensor.join()
    