import numpy as np
import pandas as pd
import json
import os
import csv
import multiprocessing
import datetime
from pre_process_feat_ext import process_feat_ext

"""
    Sensor node records values for a defined window size. In our
    case window size = 5s.
    
    We create 2 processes : Master and Slave
    
    Master node records and sends recorded values to slave node for 
    preprocessing and feature extraction.
"""

def Master(conn):
    
    reqd_cols = config_data["dataset_reqd_cols"]
    window_size = config_data['window_size']
    step_size = window_size // 2
        
    # read sensor dataset
    dataset = pd.read_csv('acc_data1.csv')
    X = dataset.iloc[:, reqd_cols].values
    
    base_TS = X[0,0]
    sensor_data = []
    window_data = {
                    "base_Timestamp" : 0,
                    "data" : []
                    }

    def send_slave(data):
        
        window_data['base_Timestamp'] = base_TS
        window_data['data'] = np.array(data[:])[:,1:]
        conn.send(window_data)
        
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
            
            base_TS += step_size
            # from cur window remove entries having timestamp < updated base_TS
            sensor_data = [rec for rec in sensor_data if rec[0] >= base_TS]         
            sensor_data.append(rec[:])
             
    
    # Once whole dataset is sent, close the connection.
    conn.send("End")
    conn.close()
    
  
def Slave(conn):

    def fetch_time_stmp():
        dt_time = datetime.datetime.now()
        day = dt_time.strftime("%d")
        month = dt_time.strftime("%m")
        hrs = dt_time.strftime("%H")
        mins = dt_time.strftime("%M")
        time_stmp = day + month + "_" + hrs + mins
        return time_stmp
    
    def file_dump(file, data):
        with open(file, 'a') as f:
            w = csv.writer(f)
            w.writerows(data)
            
    extr_feat_buf = []
    buf_rec_limit = config_data['max_buf_records']
    file_dump_limit = config_data['max_dump_count']
    buf_dump_count = 0 # no. of times buf is dumped into file
    file_name = "acc_prep_extr_feat"
    file = "{0}{1}.csv".format(file_name, fetch_time_stmp())
    
    while True:
        
        window_data = conn.recv()
        if window_data == "End":
            # check if there is data yet to be dumped into file
            if len(extr_feat_buf):
                # create new file if enough data is dumped into cur file
                if buf_dump_count == file_dump_limit:
                    file = "{0}{1}.csv".format(file_name, fetch_time_stmp())
                    
                file_dump(file, extr_feat_buf[:])
            break
        
        else:
            extr_feat = process_feat_ext(window_data, config_data)
            # if buffer is full store data into file
            if len(extr_feat_buf) == buf_rec_limit:
                # create new file if enough data is dumped into cur file
                if buf_dump_count == file_dump_limit:
                    file = "{0}{1}.csv".format(file_name, fetch_time_stmp())
                    buf_dump_count = 0
                
                file_dump(file, extr_feat_buf[:])
                extr_feat_buf = []
                buf_dump_count += 1
                
            extr_feat_buf.append(extr_feat)
                
  
if __name__ == "__main__":
       
    # read sensor config data
    with open("./acc_config.json") as fp:
        config_data = json.load(fp)
    fp.close()
    
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