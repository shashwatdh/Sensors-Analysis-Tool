import numpy as np
import pandas as pd
import datetime
import csv

def create_rec(data1, data2):
    tmp = list(data1[:])
    tmp.extend(list(data2[:]))
    return tmp

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

def rec_dump(data):
    global buf 
    global file_dump_count
    global file
    
    if len(buf) == buf_limit:
        # create new file if enough data is dumped into cur file
        if file_dump_count == file_dump_limit:
            file = "{0}{1}.csv".format(file_name, fetch_time_stmp())
            file_dump_count = 0
        
        file_dump(file, buf[:])
        buf = []
        file_dump_count += 1
                
    buf.append(data)
    
    
sensor1 = {
            'files' : ['acc_prep_extr_feat0306_2212.csv',
                       'acc_prep_extr_feat0306_2213.csv',
                       'acc_prep_extr_feat0306_2214.csv',
                       'acc_prep_extr_feat0306_2215.csv'],
            'win_size' : 5,
            'freq' : 10
        }

sensor2 = {
            'files' : ['gyro_prep_extr_feat0406_1923.csv',
                       'gyro_prep_extr_feat0406_1924.csv',
                       'gyro_prep_extr_feat0406_1927.csv',
                       'gyro_prep_extr_feat0406_1932.csv'],
            'win_size' : 5,
            'freq' : 10
        }

buf_limit = 100         # total recs that can be stored in buffer
file_dump_limit = 30    # no. of times recs can be stored in same file
file_dump_count = 0     # no. of times recs stored in same file
buf = []
file_name = "sensor1_2_merged"
file = "{0}{1}.csv".format(file_name, fetch_time_stmp())

f1_index = f2_index = 0     # index of current files
s1_win_size = sensor1['win_size']   
s2_win_size = sensor2['win_size']
s1_freq = sensor1['freq']
s2_freq = sensor2['freq']
# based on some parameter decide which file to consider for creating recs in merged file 
if (s1_freq >= s2_freq):
    file1 = sensor1['files'][0]
    file2 = sensor2['files'][0]
    data1_wsize = sensor1['win_size']   # window sizes help to determine %age overlap
    data2_wsize = sensor2['win_size']
    files1 = sensor1['files']
    files2 = sensor2['files']
else:
    file1 = sensor2['files'][0]
    file2 = sensor1['files'][0]
    data1_wsize = sensor2['win_size']
    data2_wsize = sensor1['win_size']
    files1 = sensor2['files']
    files2 = sensor1['files']

dataset = pd.read_csv(file1, header = None)
data1 = dataset.iloc[:,:].values
print(data1[0,0])
dataset = pd.read_csv(file2, header = None)
data2 = dataset.iloc[:,:].values
print(data2[0,0])
data1_ctr = data2_ctr = 0   # starting index of recs in each file 

#print(data1[5:])
#print(data2[5:])
#print(len(data2))

while(data1_ctr < len(data1) and data2_ctr < len(data2)):
    """
    If data1's window follows data2's window, there is no overlap in 
    two windows and can't be merged together so point to next rec of
    data2
    """
    rec = []  #changed
    if data1[data1_ctr][0] >= data2[data2_ctr][0] + data2_wsize:
        data2_ctr += 1
        
    else:
        # data2's window follows data1's window
        """
        If data2's window follows data1's window then there is no window in data2
        to be merged with data1's window. So if data2's window's cur index is 0 
        then it means there is a gap in two windows and we will simply ignore data1's
        window. Otherwise we will merge data1's window with data2's prev window
        and data1's index will be pointed to next window.
        """
        if data1[data1_ctr][0] + data1_wsize <= data2[data2_ctr][0]:
            if data2_ctr != 0:
                rec = create_rec(data1[data1_ctr], data2[data2_ctr - 1])
                print(str(rec[0])+","+str(rec[22]))
            data1_ctr += 1
    
        else:
            # case: when data1's window starts within data2's window but ends after data2's window
            if data1[data1_ctr][0] + data1_wsize > data2[data2_ctr][0] + data2_wsize:
                if (data2[data2_ctr][0] + data2_wsize - data1[data1_ctr][0]) / data1_wsize > 0.5:
                    rec = create_rec(data1[data1_ctr], data2[data2_ctr])
                    data1_ctr += 1
                else:
                    data2_ctr += 1
            else:
                # check if data1's window completely overlaps with data2's window
                if data1[data1_ctr][0] >= data2[data2_ctr][0]:
                    rec = create_rec(data1[data1_ctr], data2[data2_ctr])
                    data1_ctr += 1 #
                else:
                    if ((data1[data1_ctr][0] + data1_wsize) - data2[data2_ctr][0]) / data1_wsize > 0.5:
                        rec = create_rec(data1[data1_ctr], data2[data2_ctr])
                        
                    else:
                        rec = create_rec(data1[data1_ctr], data2[data2_ctr - 1])
                    
                    data1_ctr += 1 # can be merged
        if len(rec):
            #print(str(rec[0])+","+str(rec[22]))
            rec_dump(rec[:])
        
    if data1_ctr == len(data1):
        f1_index += 1
        if f1_index < len(files1):
        
            dataset = pd.read_csv(files1[f1_index])
            data1 = dataset.iloc[:,:].values
            data1_ctr = 0
            
        else:
            break
        
    if data2_ctr == len(data2):
        f2_index += 1
        if f2_index < len(files2):
                        
            dataset = pd.read_csv(files2[f2_index])
            data2 = dataset.iloc[:,:].values
            data2_ctr = 0
            
        else:
            break
        
        
while (data1_ctr < len(data1)):
    rec = create_rec(data1[data1_ctr], data2[data2_ctr - 1])
    rec_dump(rec[:])
    data1_ctr += 1

while (data2_ctr < len(data2)):
    rec = create_rec(data1[data1_ctr - 1], data2[data2_ctr])
    rec_dump(rec[:])
    data2_ctr += 1
    
    
if len(buf):
    file_dump(file, buf[:])
