import numpy as np
import pandas as pd
import json
import os
import csv
import multiprocessing
import datetime
import math
from pre_process_feat_ext import process_feat_ext


class sensor:
    
    def __init__(self, config_data):
        self.reqd_cols = config_data["dataset_reqd_cols"]
        self.window_size = config_data['window_size']
        self.step_size = math.ceil(self.window_size * config_data['window_shift'])
        #self.extr_feat_buf = []
        self.buf_rec_limit = config_data['max_buf_records']
        self.file_dump_limit = config_data['max_dump_count']
        self.file_name = config_data['file_name']
        self.keep_alive_timer = config_data['keep_alive_timer']
        self.probe_interval = config_data['probe_interval']
        self.probes_limit = config_data['probes_limit']
        #self.buf_dump_count = 0 # no. of times buf is dumped into file
        #self.file = "{0}{1}.csv".format(self.file_name, fetch_time_stmp())
    '''
    def fetch_time_stmp():
        dt_time = datetime.datetime.now()
        day = dt_time.strftime("%d")
        month = dt_time.strftime("%m")
        hrs = dt_time.strftime("%H")
        mins = dt_time.strftime("%M")
        secs = dt_time.strftime("%S")
        time_stmp = day + month + "_" + hrs + mins + secs
        return time_stmp
    '''
    
    def sense_send(self, data_file, conn):
        chunksize = 50000
        with pd.read_csv(data_file, chunksize=chunksize) as reader:
            for chunk in reader:
                #dataset = pd.read_csv(data_file)
                X = chunk.iloc[:, self.reqd_cols].values
    
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
                    if rec[0] < (base_TS + self.window_size):
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
            
                        base_TS += self.step_size
                        # from cur window remove entries having timestamp < updated base_TS
                        sensor_data = [rec for rec in sensor_data if rec[0] >= base_TS]         
                        sensor_data.append(rec[:])
             
    
                # Once whole dataset is sent, close the connection.
        conn.send("End")
        conn.close()

    
    def recv_preproc(self, conn, config_data):
        
        def cur_time_sec():
            return datetime.datetime.now().strftime("%s")
    
        def fetch_time_stmp():
            dt_time = datetime.datetime.now()
            day = dt_time.strftime("%d")
            month = dt_time.strftime("%m")
            hrs = dt_time.strftime("%H")
            mins = dt_time.strftime("%M")
            secs = dt_time.strftime("%S")
            time_stmp = day + month + "_" + hrs + mins + secs
            return time_stmp
    
        def file_dump(file, data):
            with open(file, 'a') as f:
                w = csv.writer(f)
                w.writerows(data)
     
        extr_feat_buf = []
        buf_dump_count = 0 # no. of times buf is dumped into file
        file = "{0}{1}.csv".format(self.file_name, fetch_time_stmp())
        
        while True:
        
            window_data = conn.recv()
            if window_data == "End":
                # check if there is data yet to be dumped into file
                if len(extr_feat_buf):
                    # create new file if enough data is dumped into cur file
                    if buf_dump_count == self.file_dump_limit:
                        file = "{0}{1}.csv".format(self.file_name, fetch_time_stmp())
                    
                    file_dump(file, extr_feat_buf[:])
                break
        
            else:
                extr_feat = process_feat_ext(window_data, config_data)
                # if buffer is full store data into file
                if len(extr_feat_buf) == self.buf_rec_limit:
                    # create new file if enough data is dumped into cur file
                    if buf_dump_count == self.file_dump_limit:
                        file = "{0}{1}.csv".format(self.file_name, fetch_time_stmp())
                        buf_dump_count = 0
                
                    file_dump(file, extr_feat_buf[:])
                    extr_feat_buf = []
                    buf_dump_count += 1
                
                extr_feat_buf.append(extr_feat)