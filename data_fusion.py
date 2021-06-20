import os
import numpy as np
import pandas as pd
import datetime
import csv

sensor_files = [
    {'name': "acc2",
     'files': ["acc2_prep_extr_feat1406_1314.csv",  
               "acc2_prep_extr_feat1406_1315.csv",  
               "acc2_prep_extr_feat1406_1316.csv",  
               "acc2_prep_extr_feat1406_1317.csv",  
               "acc2_prep_extr_feat1406_1318.csv",  
               "acc2_prep_extr_feat1406_1320.csv" ],
     'size':0,
     'base_file': True,
     'win_size':5,
     'freq':10},
    {'name': "acc1",
     'files': ["acc_prep_extr_feat0306_2212.csv",
               "acc_prep_extr_feat0306_2213.csv",
               "acc_prep_extr_feat0306_2214.csv",
               "acc_prep_extr_feat0306_2215.csv",
               "acc_prep_extr_feat0306_2216.csv"
               ],
     'size':0,
     'base_file': True,
     'win_size':5,
     'freq':10},
    {'name': "gyro1",
     'files': ["gyro_prep_extr_feat0406_1923.csv",  
               "gyro_prep_extr_feat0406_1924.csv",  
               "gyro_prep_extr_feat0406_1927.csv",  
               "gyro_prep_extr_feat0406_1932.csv",  
               "gyro_prep_extr_feat0406_1940.csv",  
               "gyro_prep_extr_feat0406_1950.csv"],
     'size':0,
     'base_file': True,
     'win_size':5,
     'freq':10},
    {'name': "gyro2",
     'files': ["gyro_prep_extr_feat1406_1138.csv",  
               "gyro_prep_extr_feat1406_1139.csv", 
               "gyro_prep_extr_feat1406_1140.csv",  
               "gyro_prep_extr_feat1406_1141.csv",  
               "gyro_prep_extr_feat1406_1142.csv",  
               "gyro_prep_extr_feat1406_1144.csv"],
     'size':0,
     'base_file': True,
     'win_size':5,
     'freq':10}
    ]

def fetch_time_stmp():
        dt_time = datetime.datetime.now()
        day = dt_time.strftime("%d")
        month = dt_time.strftime("%m")
        hrs = dt_time.strftime("%H")
        mins = dt_time.strftime("%M")
        secs = dt_time.strftime("%S")
        time_stmp = day + month + "_" + hrs + mins + secs
        return time_stmp

buf_limit = 100         # total recs that can be stored in buffer
file_dump_limit = 30    # no. of times recs can be stored in same file
file_dump_count = 0     # no. of times recs stored in same file
buf = []
file_name = ""
file = "{0}{1}.csv".format(file_name, fetch_time_stmp())
files_created = []  # stores intermedidate files created

def create_rec(data1, data2):
    tmp = list(data1[:])
    tmp.extend(list(data2[:]))
    return tmp



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
            files_created.append(file)
            file_dump_count = 0
        
        file_dump(file, buf[:])
        buf = []
        file_dump_count += 1
                
    buf.append(data)
    

def min_heapify(node, size, data):
    #size = len(data) - 1
    while node <= size:
        lt_child = (2 * node) + 1
        rt_child = lt_child + 1
        min_node = node
        if lt_child <= size and data[lt_child]['size'] < data[min_node]['size']:
            min_node = lt_child
        if rt_child <= size and data[rt_child]['size'] < data[min_node]['size']:
            min_node = rt_child
        if min_node != node:
            data[min_node], data[node] = data[node], data[min_node]
            node = min_node
        else:
            break
        
def build_heap(data):
    size = len(data)
    start_ind = (size // 2)
    for node in reversed(range(start_ind)):
        min_heapify(node, size - 1, data)
        
def extract_min(data):
    last_index = len(data) - 1
    #reqd_data = data[0][:]
    data[0], data[last_index] = data[last_index], data[0]
    reqd_data = data.pop()
    #size = len(data) - 1
    last_index -= 1
    min_heapify(0, last_index, data)
    return reqd_data

def heap_insert(data, new_data):
    data.append(new_data)
    cur_index = len(data) - 1
    if (cur_index % 2):
        parent_index = (cur_index - 1) // 2
    else:
        parent_index = (cur_index - 2) // 2
        
    while(cur_index > 0 and data[parent_index]['size'] > data[cur_index]['size']):
        data[parent_index], data[cur_index] = data[cur_index], data[parent_index]
        cur_index = parent_index
        if (cur_index % 2):
            parent_index = (cur_index - 1) // 2
        else:
            parent_index = (cur_index - 2) // 2
            
def file_merge(inp1, inp2):
    global files_created
    
    f1_index = f2_index = 0     # index of current files
    s1_win_size = inp1['win_size']   
    s2_win_size = inp2['win_size']
    s1_freq = inp1['freq']
    s2_freq = inp2['freq']
    # based on some parameter decide which file to consider for creating recs in merged file 
    if (s1_freq >= s2_freq):
        file1 = inp1['files'][0]
        file2 = inp2['files'][0]
        data1_wsize = inp1['win_size']   # window sizes help to determine %age overlap
        data2_wsize = inp2['win_size']
        files1 = inp1['files']
        files2 = inp2['files']
    else:
        file1 = inp2['files'][0]
        file2 = inp1['files'][0]
        data1_wsize = inp2['win_size']
        data2_wsize = inp1['win_size']
        files1 = inp2['files']
        files2 = inp1['files']

    dataset = pd.read_csv(file1)
    data1 = dataset.iloc[:,:].values

    dataset = pd.read_csv(file2)
    data2 = dataset.iloc[:,:].values

    data1_ctr = data2_ctr = 0   # starting index of recs in each file 

    global file
    file = "{0}{1}.csv".format(file_name, fetch_time_stmp())
    files_created.append(file)
    
    while(data1_ctr < len(data1) and data2_ctr < len(data2)):
        """
        If data1's window follows data2's window, there is no overlap in 
        two windows and can't be merged together so point to next rec of
        data2
        """
        rec = []
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
                        data1_ctr += 1
                    else:
                        if ((data1[data1_ctr][0] + data1_wsize) - data2[data2_ctr][0]) / data1_wsize > 0.5:
                            rec = create_rec(data1[data1_ctr], data2[data2_ctr])
                        
                        else:
                            rec = create_rec(data1[data1_ctr], data2[data2_ctr - 1])
                    
                        data1_ctr += 1
            if len(rec):
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

    #create new entry in sensor_files
    
    merged_data_inf = {}
    merged_data_inf['name'] = file_name
    merged_data_inf['files'] = files_created[:]
    tot_size = 0
    for file in merged_data_inf['files']:
        tot_size += os.stat(file).st_size
    merged_data_inf['size'] = tot_size
    if (s1_freq >= s2_freq):
        merged_data_inf['win_size'] = inp1['win_size']
        merged_data_inf['freq'] = s1_freq
    else:
        merged_data_inf['win_size'] = inp2['win_size']
        merged_data_inf['freq'] = s2_freq
    files_created = []
    return merged_data_inf

# setting size key with total size of all files of sensor
for sensor in sensor_files:
    tot_size = 0
    for file in sensor['files']:
        tot_size += os.stat(file).st_size
    sensor['size'] = tot_size
    

tot_recs = len(sensor_files)
# heapify sensor files
if tot_recs > 2:
    build_heap(sensor_files)

while tot_recs >= 2:
    
    if tot_recs == 2:
        s_files1 = sensor_files.pop(0)
        s_files2 = sensor_files.pop(0)
    
    else:
        s_files1 = extract_min(sensor_files)
        s_files2 = extract_min(sensor_files)
    """
    data1 = {}
    data1['name'] = s_files1['name']
    data1['files'] = s_files1['files']
    data1['win_size'] = 5
    data1['freq'] = 10
    
    data2 = {}
    data2['name'] = s_files2['name']
    data2['files'] = s_files2['files']
    data2['win_size'] = 5
    data2['freq'] = 10
    """
    file_name = s_files1['name'] + "_" + s_files2['name']
    print(file_name)
    new_data = file_merge(s_files1, s_files2)
    heap_insert(sensor_files, new_data)
    tot_recs = len(sensor_files)

print(sensor_files)
'''    
if len(sensor_files) == 2:
'''    