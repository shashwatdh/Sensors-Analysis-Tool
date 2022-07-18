import datetime

def fetch_time_stmp():
        dt_time = datetime.datetime.now()
        day = dt_time.strftime("%d")
        month = dt_time.strftime("%m")
        hrs = dt_time.strftime("%H")
        mins = dt_time.strftime("%M")
        time_stmp = day + month + "_" + hrs + mins
        return time_stmp
    
def df_cols(sensor_id, tot_cols):
    # returns dataframe columns
    #tot_cols = len(merge_data[sensor_id]["data"][0])
    cols = ["timestamp"]
    for i in range(1, tot_cols):
        cols.append(str(sensor_id) + "_" + str(i))
    return cols