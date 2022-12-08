import pandas as pd
import numpy as np
import os
import json
import sys
#sys.path.append(".")
from sensor import * 


class inertial_sensors:
    def __init__(self, sensor_config):#sensor_id):
         
        self.sensors_config = os.path.join(".","sensors_config.json")
        with open(self.sensors_config) as fp:
            self.config_data = json.load(fp)
        fp.close()

        self.sensor_config = sensor_config #self.config_data["sensors"][sensor_id]
        self.inertial_sensor = sensor(self.sensor_config) #sensor.sensor(self.sensor_config)
    
    def send(self):
        
        while (not self.inertial_sensor.dataset_read_completed) or (len(self.inertial_sensor.data_windows_list)):
            data = self.inertial_sensor.fetch_cur_window_and_slide_V2()
            if not len(data):
                continue
            print("fetched Data:\n", data)
            self.inertial_sensor.push_to_dataQ(data)

        self.inertial_sensor.push_to_dataQ({"base_Timestamp" : -1,
                                   "data" : "End"})
        print("abt 2 disconnect")
        while(not self.inertial_sensor.disconnected):
            continue

        print("Disconnected")
