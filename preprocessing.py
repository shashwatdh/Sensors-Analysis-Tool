import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, MaxAbsScaler, MinMaxScaler


def handle_missing_data(data, indices, technique, f_val = None):
    """
            cont_ind: maintains indices of continuous attr
            cat_ind: maintains indices of categorical attr
            params: contains technique to handle missing values in attr
    """
    imputer = SimpleImputer(missing_values=np.nan, strategy=technique, fill_value=f_val)
    imputer.fit(data[:,indices])
    data[:,indices] = imputer.transform(data[:,indices])
    #print(data)  
def attr_one_hot_encoding(data, cat_indices):
    """
            data: features containing categorical attr
            cat_indices: indices of categorical attr
    """
    ct = ColumnTransformer(transformers=[('encoder', OneHotEncoder(), cat_indices)], remainder='passthrough')
    data = np.array(ct.fit_transform(data))
    return data

def attr_label_encoding(data):
    le = LabelEncoder()
    data = le.fit_transform(data)
    return data
        
def data_split(features, label, split_prop):
    y_train = y_test = None
    if label != None:
        X_train, X_test, y_train, y_test = train_test_split(features, label, test_size = split_prop, random_state = 1)
    else:
        X_train, X_test = train_test_split(features, test_size = split_prop, random_state = 1)
    return X_train, X_test, y_train, y_test
    
def feature_scaling(X_train, X_test, indices, technique):
    scaler = None
    if technique == "min_max_scaler":
        scaler = MinMaxScaler()
    elif technique == "max_abs_scaler":
        scaler = MaxAbsScaler()
    else:
        scaler = StandardScaler()
    X_train[:,indices] = scaler.fit_transform(X_train[:,indices])
    X_test[:,indices] = scaler.transform(X_test[:,indices])
            
            
        
        
        
        
        
        
        
        
        
        