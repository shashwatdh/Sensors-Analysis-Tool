# -*- coding: utf-8 -*-
"""
Created on Sun Jan 30 23:16:30 2022

@author: shash
"""
import socket			
import json
import pandas as pd
import numpy as np
# Create a socket object
s = socket.socket()		

# Define the port on which you want to connect
port = 65432			

# connect to the server on local computer
s.connect(('127.0.0.1', port))

# receive data from the server and decoding to get the string.
print (s.recv(1024).decode())

"""
s.send(dir_path.encode())
# close the connection
data = s.recv(1048576)
print(json.loads(data))
s.close()	
	"""
    
dataset = pd.read_csv('acc2_small.csv')
X = dataset.iloc[:,:].values
base_TS = X[0,0]
sensor_data = []
window_size = 5
step_size = 2
"""
    Client sends data packet containing window data, client id and base_timestamp.
    base_timestamp can help us in in validating if data can be merged with other 
    sensors' data.
"""

window_data = {
    "client_id":"Acc_1",
    "base_Timestamp" : base_TS,
    "data" : []
    }

def send_slave(data):
        
        window_data['base_Timestamp'] = base_TS
        window_data['data'] = np.array(data[:])[:,:].tolist()
        s.send(json.dumps(window_data).encode())

for rec in X:
        
        # storing values recorded in a time frame into a list
        if rec[0] < (base_TS + window_size):
            sensor_data.append(rec[:]) 
        
        else:
            if len(sensor_data):
                send_slave(sensor_data[:])
            
            base_TS += step_size
            # from cur window remove entries having timestamp < updated base_TS
            sensor_data = [rec for rec in sensor_data if rec[0] >= base_TS]         
            sensor_data.append(rec[:])
#s.send("END".encode())
s.close()