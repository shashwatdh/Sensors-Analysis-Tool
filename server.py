# -*- coding: utf-8 -*-
"""
Performs model training and cross validation

Receives dataset path from client.py

"""

import numpy as np
import pandas as pd
import json
import socket            
from CrossValidation import *
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from CV import cross_validation
from sklearn.metrics import accuracy_score
# create a socket object
s = socket.socket()        
print ("Socket successfully created")
 
# reserve a port on your computer in our
# case it is 12345 but it can be anything
port = 12345               
 
# Next bind to the port
# we have not typed any ip in the ip field
# instead we have inputted an empty string
# this makes the server listen to requests
# coming from other computers on the network
s.bind(('', port))        
print ("socket binded to %s" %(port))
 
# put the socket into listening mode
s.listen(5)    
print ("socket is listening")           
 
# a forever loop until we interrupt it or
# an error occurs
while True:
 
# Establish connection with client.
    c, addr = s.accept()    
    print ('Got connection from', addr )
 
    # send a thank you message to the client. encoding to send byte type.
    c.send('Thank you for connecting'.encode())
    
    while True:
        dir_path = c.recv(1024).decode()
        if len(dir_path):
            print(dir_path)
            
            #Read Dataset
            train_df = pd.read_csv(dir_path + "\\train\\small_train.csv")
            test_df = pd.read_csv(dir_path + "\\test\\small_test.csv")
            
            #Read config files
            with open(".\\ml_conf.json") as fp:
                ml_config_data = json.load(fp)
            fp.close()
            
            with open(".\\CV_config.json") as fp:
                config_data = json.load(fp)
            fp.close()
            
            #Read cols to be dropped
            cols_drop = ml_config_data["drop_cols"]
            
            #dependent attr
            y_attr = ml_config_data["y_attr"]   
                     
            
            x_train = train_df.drop(cols_drop, axis = 1)
            y_train = train_df[y_attr]

            x_test = test_df.drop(cols_drop, axis = 1)
            y_test = test_df[y_attr]
            
            #Set cross-validation paramters
            if config_data["technique"] == "GroupKFold" or config_data["technique"] == "StratifiedGroupKFold":
                grp_attr = config_data["group_id_col"]
                #groups = np.array(train_df['sub_id'])
                groups = np.array(train_df[grp_attr])
                tot_groups = len(np.unique(groups))
                config_data["n_splits"] = tot_groups
                config_data["group_labels"] = groups


            data_splits = []
    
            if config_data["technique"] == "GroupKFold":
                gkf = Group_K_Fold_CV(config_data["n_splits"], x_train, y_train, config_data["group_labels"])
                data_splits = gkf.Group_K_Fold()


            elif config_data["technique"] == "StratifiedGroupKFold":
                sgk = Stratified_GRP_K_Fold_CV(config_data["n_splits"], config_data["shuffle"],
                                           config_data["random_state"],
                                               x_train, y_train)
                data_splits = sgk.SGK_Fold()

            elif config_data["technique"] == "KFold":
                kf = K_Fold_CV(config_data["n_splits"], config_data["shuffle"],
                       config_data["random_state"], x_train, y_train)
                data_splits = kf.K_Fold()
    
            elif config_data["technique"] == "StratifiedKFold":
    
                skf = Stratified_K_Fold_CV(config_data["n_splits"], config_data["shuffle"],
                                    config_data["random_state"], x_train, y_train)
    
                data_splits = skf.SK_Fold()

            #Set model parameters and train the model
            model = ml_config_data["model"]
            if model == "DT":
                criterion = ml_config_data["parameters"]["criterion"]  
                splitter = ml_config_data["parameters"]["splitter"]
                max_depth = eval(ml_config_data["parameters"]["max_depth"])
                min_samples_split = ml_config_data["parameters"]["min_samples_split"]
                min_samples_leaf = ml_config_data["parameters"]["min_samples_leaf"]
                min_weight_fraction_leaf = ml_config_data["parameters"]["min_weight_fraction_leaf"]
                max_features = ml_config_data["parameters"]["max_features"]
                max_features = None if max_features == 'None' else max_features
                random_state = ml_config_data["parameters"]["random_state"]
                random_state = None if random_state == 'None' else random_state
                max_leaf_nodes = eval(ml_config_data["parameters"]["max_leaf_nodes"])
                min_impurity_decrease = ml_config_data["parameters"]["min_impurity_decrease"]
                class_weight = ml_config_data["parameters"]["class_weight"]
                class_weight= None if class_weight == 'None' else class_weight
                ccp_alpha = ml_config_data["parameters"]["ccp_alpha"]
                check_input = ml_config_data["parameters"]["check_input"]
                
                clf = DecisionTreeClassifier(criterion = criterion, splitter = splitter, max_depth = max_depth,
                                             min_samples_split = min_samples_split,
                                             min_samples_leaf = min_samples_leaf,
                                             min_weight_fraction_leaf = min_weight_fraction_leaf,
                                             max_features = max_features,
                                             random_state = random_state,
                                             max_leaf_nodes = max_leaf_nodes,
                                             min_impurity_decrease = min_impurity_decrease,
                                             class_weight = class_weight,
                                             ccp_alpha = ccp_alpha)
            
            
            elif model == "kNN":
                n_neighbors = ml_config_data["parameters"]["n_neighbors"] 
                wts = ml_config_data["parameters"]["weights"]
                algo = ml_config_data["parameters"]["algorithm"]
                l_size = ml_config_data["parameters"]["leaf_size"]
                p = ml_config_data["parameters"]["p"]
                metric = ml_config_data["parameters"]["metric"]
                m_params = ml_config_data["parameters"]["metric_params"]
                n_jobs = ml_config_data["parameters"]["n_jobs"]
                
                clf = KNeighborsClassifier(n_neighbors = n_neighbors, weights = wts,
                                           algorithm = algo, leaf_size = l_size,
                                           p = p, metric = metric, metric_params = m_params,
                                           n_jobs = n_jobs)
            
            #clf.fit(x_train, y_train)
            
            def apply_model(x_train, y_train, x_test, y_test):
    
                clf_predict = np.array([])
                clf.fit(x_train, y_train)
                clf_predict = clf.predict(x_test)
    
                accuracy = np.round(accuracy_score(y_test, clf_predict)*100, 2)
                print(accuracy)

            
            for X_train, X_test, Y_train, Y_test in data_splits:
                apply_model(X_train, Y_train, X_test, Y_test)
                
            break
      # Close the connection with the client
    c.close()
   
    # Breaking once connection closed
    break