from multiprocessing import Process, Queue
import numpy as np
import pandas as pd
import time
import sys
import json

def file_merge(queue, sensors_count, sensor_state):
    ## Read from the queue; this will be spawned as a separate Process
    data_merged = False
    while True:
        msg = queue.get()         # Read from the queue and do nothing
        if (msg["data"] == 'DONE'):
            print("sensor-" + str(msg["sensor_id"]) + " terminating....")
            sensors_count -= 1
            if not sensors_count:
                break
        else:
            # Fetch message from queue 
            print(msg)
            sensor_id = msg["sensor_id"]
            base_TS = msg["base_Timestamp"]
            data = msg["data"]
            
            if data_merged:
                """ 
                    If data is rcvd just after merging old sensor data,  
                    simply overwrite old sensor data.
                """ 
                sensor_state[msg["sensor_id"]] = msg
                data_merged = False
            else:
                pass
            
            
            

def sensor_data(queue, config):
    print("Sensor - " + str(config["id"]) + "about to send data...")
    #sensor_id = config["id"]
    window_size = config["window_size"]
    step_size = config["step_size"]
    reqd_cols = [0,2,3,4]
    # read sensor dataset
    dataset = pd.read_csv(config["dataset_path"])
    X = dataset.iloc[:, reqd_cols].values
    
    base_TS = X[0,0]
    sensor_data = []
    window_data = {
                    "sensor_id" : config["id"],
                    "base_Timestamp" : 0,
                    "window_size" : window_size,
                    "data" : []
                    }

    def send_slave(data):
        
        window_data['base_Timestamp'] = base_TS
        window_data['data'] = np.array(data[:])[:,1:]
        queue.put(window_data)
        
    for rec in X:
        
        # storing values recorded in a time frame into a list
        if rec[0] < (base_TS + window_size):
            sensor_data.append(rec[:]) 
        
        else:
            
            '''
            Record belongs to different window. So first send populated data
            to slave process for pre-processing and feature extraction.
            
            Set base_TS value and clear window_data. Add new entry in 
            window_data.
            '''
            
            if len(sensor_data):
                send_slave(sensor_data[:])
            
            base_TS += (window_size * step_size)
            # from cur window remove entries having timestamp < updated base_TS
            sensor_data = [rec for rec in sensor_data if rec[0] >= base_TS]         
            sensor_data.append(rec[:])
             
    
    # Once whole dataset is sent, close the connection.
    #conn.send("End")
    #conn.close()
    
    queue.put('DONE')

if __name__=='__main__':
    with open(".\\sensors_config.json") as fp:
        config_data = json.load(fp)
    fp.close()
    
    sensor_data = config_data["sensors"]
    #print(sensor_data)
    pqueue = Queue()
    
    sensor_processes = []
    sensor_state = {}
    for sensor_config in sensor_data:
        print(sensor_config)
        # Set sensor state
        sensor_state[sensor_config["id"]] = None
        # Creating and starting child processes     
        sensor_processes.append(Process(target=sensor_data, args=(pqueue,sensor_config)))
        #sensor_processes[-1].start()
        
    for child in sensor_processes:
        child.start()
        
    print("Client processes created!!!")
    
    file_merge(pqueue, len(sensor_processes), sensor_state)
    
    for child in sensor_processes:
        child.join()
    """
    for child_index in range(len(sensor_data)):
        sensor_processes[child_index].join()"""
    """
    pqueue = Queue() # writer() writes to pqueue from _this_ process
    for count in [10**4, 10**5, 10**6]:             
        ### reader_proc() reads from pqueue as a separate process
        reader_p = Process(target=reader_proc, args=((pqueue),))
        reader_p.daemon = True
        reader_p.start()        # Launch reader_proc() as a separate python process

        _start = time.time()
        writer(count, pqueue)    # Send a lot of stuff to reader()
        reader_p.join()         # Wait for the reader to finish
        print("Sending {0} numbers to Queue() took {1} seconds".format(count, 
            (time.time() - _start)))"""