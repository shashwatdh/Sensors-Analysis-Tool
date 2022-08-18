def missing_data_impute_exp(last_window, next_window, cur_baseTS):
    
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
    last_window_data = last_window["data"][:]
    
    next_window_baseTS = next_window["base_Timestamp"]
    next_window_data = next_window["data"][:]
    
    # 1. Calculate interval between last_pck and nxt_pkt
    baseTS_diff = next_window_baseTS - last_window_baseTS
    
    # 2. calculate gap between cur baseTS and last and nxt pkt baseTS.
    cur_last_baseTS_diff = cur_baseTS - last_window_baseTS
    next_cur_baseTS_diff = next_window_baseTS - cur_baseTS
    
    # 3. Based on len of pkts determine the index for merge_asof and acc
    #    assign wts.
    
    def assign_wt(curTS_diff, pktsTS_diff):
        return 1 - (curTS_diff / pktsTS_diff)
        
    last_window_wt = assign_wt(cur_last_baseTS_diff, baseTS_diff)
    next_window_wt = assign_wt(next_cur_baseTS_diff, baseTS_diff)
    
    if len(next_window_data) > len(last_window_data):
        (left_df, right_df) = (next_window_data, last_window_data)
        (left_df_wt, right_df_wt) = (next_window_wt, last_window_wt)    
    else:
        (left_df, right_df) = (last_window_data, next_window_data)
        (left_df_wt, right_df_wt) = (last_window_wt, next_window_wt)
    
    
     
    
     
                        
    
    