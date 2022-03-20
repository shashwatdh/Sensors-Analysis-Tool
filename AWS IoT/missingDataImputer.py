import pandas as pd

def missing_data_impute_exp(last_window, next_window, cur_baseTS, cols):
    
    """
        Imputes missing window data using exponential technique.
        
        To extrapolate data for current baseTS window :
        1) calculate interval between last_pck and nxt_pkt
        2) calculate gap between cur baseTS and last and nxt pkt baseTS.
        3) Based on gap assign wts to each packet.
        4) modify timestamp of each packet to cur window's TS.
        5) merge the two pkts. pkt with more data must be set as index.
    """
    
    
    last_window_baseTS = last_window["base_Timestamp"]
    last_window_data = pd.DataFrame(last_window["data"][:], columns=cols)
    
    next_window_baseTS = next_window["base_Timestamp"]
    next_window_data = pd.DataFrame(next_window["data"][:], columns=cols)
    
    #print("last_pkt:",last_window_data.loc[:,:])
    #print("next_pkt:",next_window_data.loc[:,:])
    #last_window_data = pd.DataFrame(last_window_data, columns=cols)
    #next_window_data = pd.DataFrame(next_window_data, columns=cols)
    # 1. Calculate interval between last_pck and nxt_pkt
    baseTS_diff = next_window_baseTS - last_window_baseTS
    
    # 2. calculate gap between cur baseTS and last and nxt pkt baseTS.
    cur_last_baseTS_diff = cur_baseTS - last_window_baseTS
    next_cur_baseTS_diff = next_window_baseTS - cur_baseTS
    
    # 3. Based on len of pkts determine the index for merge_asof and acc
    #    assign wts.
    
    def assign_wt(curTS_diff, pktsTS_diff):
        return 1 - (curTS_diff / pktsTS_diff)
        
    last_window_wt = (assign_wt(cur_last_baseTS_diff, baseTS_diff), 0) [last_window_baseTS < 0]
    next_window_wt = (assign_wt(next_cur_baseTS_diff, baseTS_diff), 0) [next_window_baseTS < 0]
    
    # 4. Edit Timestamp of both pkts to match cur_win TSs.
    last_window_data.loc[:, "timestamp"] += cur_last_baseTS_diff
    next_window_data.loc[:, "timestamp"] -= next_cur_baseTS_diff
    
    # Based on window len, select left_index
    if len(next_window_data) > len(last_window_data):
        (left_df, right_df) = (next_window_data, last_window_data)
        (left_df_wt, right_df_wt) = (next_window_wt, last_window_wt)    
    else:
        (left_df, right_df) = (last_window_data, next_window_data)
        (left_df_wt, right_df_wt) = (last_window_wt, next_window_wt)
    
    #cols_skip = left_df.shape[1]
    
    # 5. merge the dataframes
    if left_df_wt and right_df_wt:
        cols_skip = left_df.shape[1]
        df_merged = pd.merge_asof(left_df, right_df, on = "timestamp", tolerance = 1.0, direction = "backward")
        
        # 6. Calcultate aggr window data
        aggr_win = pd.DataFrame((left_df_wt * df_merged.iloc[:, 1:cols_skip].values) + 
                     (right_df_wt * df_merged.iloc[:,cols_skip:].values))
        #print("aggr_win:",aggr_win, len(aggr_win), aggr_win.isnull().values.all())
        if aggr_win.isnull().values.all():
            return None
        return pd.concat([df_merged.loc[:,"timestamp"],
                      pd.DataFrame((left_df_wt * df_merged.iloc[:, 1:cols_skip].values) + 
                                   (right_df_wt * df_merged.iloc[:,cols_skip:].values))], axis=1)
    
    elif not left_df_wt:
        #print("Only right df")
        return pd.concat([right_df.loc[:,"timestamp"],
                          pd.DataFrame((right_df_wt * right_df.iloc[:,1:].values))], axis=1)
    
    else:
        #print("only left df")
        return pd.concat([left_df.loc[:,"timestamp"],
                      pd.DataFrame((left_df_wt * left_df.iloc[:, 1:].values))], axis=1)
    
     
                        
    
    