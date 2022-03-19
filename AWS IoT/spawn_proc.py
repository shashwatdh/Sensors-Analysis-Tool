# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 21:22:24 2022

@author: shash
"""
import os, subprocess
"""
subprocess.Popen("python merge_node.py")
subprocess.Popen("python iot_sensor_acc1.py")
subprocess.Popen("python iot_sensor_gyro1.py")"""

os.system("python merge_node.py")
os.system("python iot_sensor_acc1.py")
os.system("python iot_sensor_gyro1.py")
