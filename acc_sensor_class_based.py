import numpy as np
import pandas as pd
import json
import os
import csv
import multiprocessing
import datetime
import math
from pre_process_feat_ext import process_feat_ext
from sensors import sensor

def Master(conn):
    #obj = sensor(config_data)
    obj.sense_send('acc2_data.csv', conn)
    
def Slave(conn):
    #obj = sensor(config_data)
    obj.recv_preproc(conn, config_data)
    
    
if __name__ == "__main__":
       
    # read sensor config data
    with open("./acc_config.json") as fp:
        config_data = json.load(fp)
    fp.close()
    obj = sensor(config_data)
    
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