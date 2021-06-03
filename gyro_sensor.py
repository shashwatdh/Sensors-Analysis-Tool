import numpy as np
import pandas as pd
import json
import os
import csv
import multiprocessing
from pre_process_feat_ext import process_feat_ext
"""
    Sensor node records values for a defined window size. In our
    case window size = 1s.
    
    We create 2 processes : Master and Slave
    
    Master node records and sends rec values to slave node for 
    preprocessing and feature extraction.
"""

def Master(conn, reqd_cols):
    
    dataset = pd.read_csv('gyro_data1.csv')
    X = dataset.iloc[:, reqd_cols].values
    base_TS = X[0,0]
    window_data = []
    
    config = {'y_label' : 'No',
          'char_encoding': {'one_hot_encoder': [],
                            'label_encoder':[]
                            },
          'missing_data' : {'Numerical' : 'mean',
                            'Categorical' : 'most_frequent',
                            'fill_value': None},
          'test_split': 0.2,
          'Normalization': None,
          'feat_ext' : [0,1,2],
          'data': None}
    
    def send2slave(data):
        config['data'] = np.array(data[:])
        #print(config['data'])
        conn.send(config)
        
    for rec in X:
        # populating values recorded in a time frame
        if rec[0] < (base_TS + 5):
            window_data.append(rec[1:]) # removing timestamp
        else:
            """
            Record belongs to different window. So first send populated data
            to slave process for pre-processing and feature extraction.
            
            Set base_TS value and clear window_data. Add new entry in 
            window_data.
            """
            
            if len(window_data):
                send2slave(window_data[:])
            
            base_TS += 5
            window_data = []
            window_data.append(rec[1:]) # removing timestamp
             
    
    # Once data is sent close the connection.
    conn.send("End")
    conn.close()
  
def Slave(conn):

    while 1:
        config_data = conn.recv()
        if config_data == "End":
            break
        else:
            X = config_data['data']
            extr_feat = process_feat_ext(config_data)
            # print extr_feat to file
            print(extr_feat)
            if(len(extr_feat)):
                with open('gyro_prep_extr_feat.csv', 'a') as f:
                    w = csv.writer(f)
                    w.writerow(extr_feat)
        #print("Received the message: {}".format(config_data))
  
if __name__ == "__main__":
   
    reqd_cols = [0,2,3,4]
    # creating a pipe
    master_conn, slave_conn = multiprocessing.Pipe()
  
    # creating new processes
    p1 = multiprocessing.Process(target=Master, args=(master_conn, reqd_cols,))
    p2 = multiprocessing.Process(target=Slave, args=(slave_conn,))
  
    # running processes
    p1.start()
    p2.start()
  
    # wait until processes finish
    p1.join()
    p2.join()