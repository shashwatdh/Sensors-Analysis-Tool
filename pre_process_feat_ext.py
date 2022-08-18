"""
    Data preprocessing module.
    Input:
        1) Window of sensor data
        2) Config file
    
    Accepts a window of sensor data and performs following tasks:
        1) Handles missing data
        2) Encoding categorical data
        3) Split data into train and test set
        4) Normalization
"""

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from preprocessing import *
from scipy.stats import moment

"""
Its possible that in a time frame single rec is sensed by sensor. We will 
only find mean.
"""

def process_feat_ext(window_data, config):
    
    X = y = None 
    # read data
    dataset = window_data['data']
    base_TS = window_data['base_Timestamp']
    features_ext = config['feat_ext'][:]
      
    if config['y_label'] == 'Yes':
        X = dataset[:, :-1]
        y = dataset[:, -1]
    else:
        X = dataset[:,:]


    # if single data is recorded in window 
    if dataset.shape[0] == 1:
        
        feature_ext_data = [base_TS]
        
        feat_mean = np.mean(X[:,features_ext], axis = 0)
        feature_ext_data.extend(feat_mean)
        
        feat_std = feat_mom3 =  feat_mom4 = feat_perc25 = feat_perc50 = feat_perc75 = [0.0] * len(features_ext)
        feature_ext_data.extend(feat_std)
        feature_ext_data.extend(feat_mom3)
        feature_ext_data.extend(feat_mom4)
        feature_ext_data.extend(feat_perc25)
        feature_ext_data.extend(feat_perc50)
        feature_ext_data.extend(feat_perc75)
        
        return feature_ext_data

    continuous_ind = []
    categorical_ind = []
    cols = X.shape[1]

    # store indices of categorical attr
    for index in range(cols):
        if type(X[:, index][0]) == str:
            categorical_ind.append(index)
        else:
            continuous_ind.append(index)

    # Handle missing data

    """
    Parse through the columns of dataset to determine type of attribute.
    Maintain indices of continuous and categorical attr in sep lists and 
    handle missing data accordingly.
    """

    # doesn't handle the case when y_label contains null values

    if len(categorical_ind):
        handle_missing_data(X, categorical_ind, config['missing_data']['Categorical'])
    
    if len(continuous_ind):
        handle_missing_data(X, continuous_ind, 
                            config['missing_data']['Numerical'],
                            config['missing_data']['fill_value'])
    
    #print("After handling missing data:")
    #print(X)

    # Encode categorical data
    # To preserve the index, first perform label encoding

    if len(config['char_encoding']['label_encoder']):
        for index in config['char_encoding']['label_encoder']:
            if index == (cols - 1) and config['y_label'] == 'Yes':
                # encoding dependent var
                y = attr_label_encoding(y)
            else:
                # encoding independent var
                X[:, index] = attr_label_encoding(X[:, index])

    # perform one_hot_encoding
    if len(config['char_encoding']['one_hot_encoder']):
        # finding unique classes in categorical data
        one_hot_enc_unq = []
        for index in config['char_encoding']['one_hot_encoder']:
            one_hot_enc_unq.append(len(np.unique(X[:,index])))
        X = attr_one_hot_encoding(X, categorical_ind)

    """
    After applying one_hot_encoder extra attributes will added in front.
    So attr index will change. so we need to edit indices of cont attr.
    """
    #print(one_hot_enc_unq)

    if len(config['char_encoding']['one_hot_encoder']):
        re_arranged_ind = [] 
        addl_attr = sum(one_hot_enc_unq)

        for index in range(cols):
            if index in config['char_encoding']['one_hot_encoder']:
                if index == 0:
                    re_arranged_ind.append(addl_attr - 1)
                else:
                    re_arranged_ind.append(re_arranged_ind[-1])
            else:
                if index == 0:
                    re_arranged_ind.append(addl_attr)
                else:
                    re_arranged_ind.append(re_arranged_ind[-1] + 1)
                    
        # updating indices of cont attr 
        for index in range(len(continuous_ind)):
            continuous_ind[index] = re_arranged_ind[continuous_ind[index]]
        
        # updating feature extraction indices 
        for index in range(len(features_ext)):
            features_ext[index] = re_arranged_ind[features_ext[index]]
        
    # Splitting the dataset into the Training set and Test set
    X_train, X_test, y_train, y_test = data_split(X, y, config['test_split'])

    """
    # Feature Scaling    
    feature_scaling(X_train, X_test, continuous_ind, config['Normalization'])
    """
    

    # feature extraxtion
    feature_ext_data = [base_TS]

    feat_mean = np.mean(X_train[:,features_ext], axis = 0)
    feature_ext_data.extend(feat_mean)

    feat_std = np.std(X_train[:,features_ext], axis = 0, dtype = np.float64)
    feature_ext_data.extend(feat_std)

    feat_mom3 = moment(X_train[:,features_ext], moment = 3, axis = 0)
    feature_ext_data.extend(feat_mom3)

    feat_mom4 = moment(X_train[:,features_ext], moment = 4, axis = 0)
    feature_ext_data.extend(feat_mom4)

    feat_perc25 = np.percentile(X_train[:,features_ext], 25, axis = 0)
    feature_ext_data.extend(feat_perc25)

    feat_perc50 = np.percentile(X_train[:,features_ext], 50, axis = 0)
    feature_ext_data.extend(feat_perc50)

    feat_perc75 = np.percentile(X_train[:,features_ext], 75, axis = 0)
    feature_ext_data.extend(feat_perc75)

    return feature_ext_data


