import socket
import pandas as pd
import numpy as np
import json

s = socket.socket()        
print ("Socket successfully created")
 
# reserve a port on your computer in our
# case it is 12345 but it can be anything
port = 65432               
 
# Next bind to the port
# we have not typed any ip in the ip field
# instead we have inputted an empty string
# this makes the server listen to requests
# coming from other computers on the network
#s.bind(('', port))        
#print ("socket binded to %s" %(port))
 
# put the socket into listening mode
#s.listen(5)    
#print ("socket is listening")           
 
# a forever loop until we interrupt it or
# an error occurs

tot_sensors_merge = 2
sensors_detected = 0
sensors_state = {}

def old_data(cur_client_data):
    """
        check if some sensor has old data
    """
    cur_client_id = cur_client_data['client_id']
    for client in sensors_state.keys():
        if client != cur_client_id:
            if cur_client_data['base_Timestamp'] > (sensors_state['client_id']['base_Timestamp'] 
                                                    + sensors_state['client_id']['window_size']):
                return True
    
def merge_data(data):
    # Extract packet data
    data = json.loads(data)
    client_id = data['client_id']
    window_data = data['data']
    timestamp = data["base_Timestamp"]
    """
        check if data can be merged with existing state of different sensors.
        
        Initially state will be null and we must set state for each sensor.
        
        If we don't have updated data from all sensors then we must append data
        for particular sensor.
    """
    if client_id not in sensors_state:
        global sensors_detected, sensors_state
        sensors_detected += 1
        sensors_state[client_id] = {}
        sensors_state[client_id]["cur_base_timestamp"] = timestamp
        sensors_state[client_id]["data"] = window_data
        sensors_state[client_id]["window_size"] = 5 # must be read from config
        if(sensors_detected == tot_sensors_merge):
            # merge data
            pass
    
    else:
        """
            If sensor's state is already stored then we must check if we have
            updated state of all sensors. 
            
            We won't have updated state of all sensors if for some sensor no data
            has been received or if data is old.
        """
        if sensors_detected < tot_sensors_merge:
            """
             Checks if for some sensor still no data has been received.
             
             In this case we need to append the data.
            """
            sensors_state[client_id]["data"].extend(window_data)
        elif old_data(data):
            """
                We have data of all sensors. But if some sensor has old data then 
                we will have to append data
            """
            sensors_state[client_id]["data"].extend(window_data)
        else:
            """
             sensor data can be merged with other sensors' data. So we just need
             to overwrite data in dictionary for particular sensor.
            """
            sensors_state[client_id]["cur_base_timestamp"] = timestamp
            sensors_state[client_id]["data"] = window_data
            # merge the data

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind(('', port))
    s.listen()
    conn, addr = s.accept()
    conn.send('Thank you for connecting'.encode())
    with conn:
        print('Connected by', addr)
        while True:
            data = conn.recv(1048576)
            print(data)
            merge_data(data)
            if not data:
                break
            #conn.sendall(data)

"""
while True:
 c, addr = s.accept() 
 print ('Got connection from', addr )
 
 while True:
     data = c.recv()
     
"""