import numpy as np
import pandas as pd
import json
from functools import reduce
"""
a = pd.read_csv("acc1_small.csv", names = ["a_timestamp", "x","y","z"])
b = pd.read_csv("acc2_small.csv", names = ["b_timestamp", "x","y","z"])
c = pd.read_csv("gyro1_small.csv", names = ["c_timestamp", "x","y","z"])
d = pd.read_csv("gyro2_small.csv", names = ["d_timestamp", "x","y","z"])
"""

a = pd.read_csv("acc1_small.csv", names = ["timestamp", "a_x","a_y","a_z"])
b = pd.read_csv("acc2_small.csv", names = ["timestamp", "b_x","b_y","b_z"])
c = pd.read_csv("gyro1_small.csv", names = ["timestamp", "c_x","c_y","c_z"])
d = pd.read_csv("gyro2_small.csv", names = ["timestamp", "d_x","d_y","d_z"])

e = pd.read_csv("acc1_small.csv", names = ["timestamp", "a_x","a_y","a_z"], chunksize=(10))
d = e.get_chunk(10)
d1 = e.get_chunk(10)
d2 = e.get_chunk(10)
if len(d2) < 10:
    print("lst")
try:
    d3 = e.get_chuk(10)
except StopIteration:
    print("stopppp")
else:
    print("else bofy")
    
"""
merged_data = pd.merge_asof(a,c, left_on = "a_timestamp",  right_on = "c_timestamp",
                            tolerance = 2, direction = "backward")
merged_data_filter=merged_data[(merged_data.a_timestamp - merged_data.c_timestamp) <= 2]

"""
x = a.values
y = c.values
 
for i in x:
    i[0] *= 1.0

for i in y:
    i[0] *= 1.0
a1 = pd.DataFrame(x, columns=["timestamp", "a_x","a_y","a_z"])
c1 = pd.DataFrame(y, columns=["timestamp", "c_x","c_y","c_z"])

a2 = pd.DataFrame([], columns=["timestamp", "a2_x","a2_y","a2_z"])
c2 = pd.DataFrame([], columns=["timestamp", "c2_x","c2_y","c2_z"])

dfs = [a1,c1,a2,c2]
df_merged = reduce(lambda  left,right: 
                   pd.merge_asof(left,right,on = "timestamp",tolerance = 2.0, direction = "backward")
                   if (len(left) and len(right))
                   else(
                       pd.concat([left, right.iloc[:,1:]], axis = 1) if (len(left) and not len(right))
                       else(
                           pd.concat([left.iloc[:,1:], right], axis = 1) if (not len(left) and len(right))
                           else
                               pd.concat([left.iloc[:,1:], right.iloc[:,1:]], axis = 1)
                           )
                       ), dfs)


#merged = reduce(lambda left,right : )

#df_merged_list = df_merged.values.tolist()
"""
df_merged.to_csv("sensors_merged_data_try",mode="a", header=True)
df_merged.to_csv("sensors_merged_data_try",mode="a", header=True)"""