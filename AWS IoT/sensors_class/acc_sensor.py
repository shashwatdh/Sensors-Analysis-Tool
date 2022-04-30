import pandas as pd
import numpy as np
import os
import json
from sensor import sensor 

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

print("Disconnected")
