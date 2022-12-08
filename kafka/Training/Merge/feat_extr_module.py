import numpy as np
import pandas as pd

def extract_statistical_feat(merged_data, config):
    """
    for feat in list(config.keys()):
        cols = merged_data"""
    print(merged_data)
    #extr_feat = [merged_data.loc[0,"timestamp"]]
    header = merged_data.columns.tolist()
    #data = merged_data.values
    extr_head = []
    extr_feat = []
    
    def aug_header(cols, feature):
        # creates header of format - col_feat
        for col in cols:
            extr_head.append(col + "_" + feature)
        
    for feat in list(config.keys()):
        
        cols = config[feat]
        if len(cols):
            if cols[0] == "all":
                cols = header[1:]
        else:
            continue
        
        data = merged_data.loc[:,cols].values
        
        if feat == "mean":    
            feat_mean = np.mean(data, axis = 0)
            extr_feat.extend(feat_mean)
            #feat_mean = np.mean(X_train[:,features_ext], axis = 0)
    
        elif feat == "std":
            feat_std = np.std(data, axis = 0, dtype = np.float64)
            extr_feat.extend(feat_std)
        elif feat == "min":
            feat_min = np.min(data, axis=0)
            extr_feat.extend(feat_min)
        elif feat == "max":
            feat_max = np.max(data, axis = 0)
            extr_feat.extend(feat_max)
        
        aug_header(cols, feat)
    
    return extr_feat, extr_head