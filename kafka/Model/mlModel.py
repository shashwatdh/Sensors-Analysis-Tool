# -*- coding: utf-8 -*-
"""
Performs model training and cross validation

Receives dataset path from client.py

"""
import os
import numpy as np
import pandas as pd
import json
import socket            
from CrossValidation import *
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from CV import cross_validation
from sklearn.metrics import accuracy_score
import matplotlib.pyplot as plt
from sklearn.metrics import (precision_score, recall_score,f1_score, accuracy_score,confusion_matrix)
import json
from kafka import KafkaConsumer
from kafka import KafkaProducer

subTopic = "model_training_validation"
consumer = KafkaConsumer(subTopic, bootstrap_servers='localhost:9092')
dataset = ""

for msg in consumer:
    dataset = json.loads(msg.value.decode("utf-8"))["dataset"]
    dataset = os.path.join("..","feature_extr", dataset)
    print(dataset)
    break

#Read Dataset
train_df = pd.read_csv(dataset)
#test_df = pd.read_csv("feat_ext_2309_0032.csv")

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

print(x_train, y_train)

#x_test = test_df.drop(cols_drop, axis = 1)
#y_test = test_df[y_attr]

tot_groups = 1

#Set cross-validation paramters
if config_data["technique"] == "GroupKFold" or config_data["technique"] == "StratifiedGroupKFold":
    grp_attr = config_data["group_id_col"]
    #groups = np.array(train_df['sub_id'])
    groups = np.array(train_df[grp_attr])
    tot_groups = len(np.unique(groups))
    config_data["n_splits"] = tot_groups
    config_data["group_labels"] = groups


data_splits = []
grp_ids = []
if config_data["technique"] == "GroupKFold":
    gkf = Group_K_Fold_CV(config_data["n_splits"], x_train, y_train, config_data["group_labels"])
    data_splits, grp_ids = gkf.Group_K_Fold()

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
            
performance = {"grp_metrics":{}, "overall_metrics":{}}
tot_acc = tot_precision = tot_recall = 0
def apply_model(x_train, y_train, x_test, y_test, split):#, grp_id):
    
                clf_predict = np.array([])
                clf.fit(x_train, y_train)
                clf_predict = clf.predict(x_test)
                
                conf_matrix = confusion_matrix(y_test, clf_predict)
                precision = np.round(precision_score(y_test, clf_predict, average='micro')*100, 2)
                recall = np.round(recall_score(y_test, clf_predict, average='micro')*100, 2)
                accuracy = np.round(accuracy_score(y_test, clf_predict)*100, 2)
                
                performance["grp_metrics"][split] = {
                    "confusion_matrix":conf_matrix.tolist(),
                    "precision":precision,
                    "recall":recall,
                    "accuracy":accuracy
                    }
                
                global tot_acc, tot_precision, tot_recall
                
                tot_acc += accuracy
                tot_precision += precision
                tot_recall += recall
                
                fig, ax = plt.subplots(figsize=(7.5, 7.5))
                ax.matshow(conf_matrix, cmap=plt.cm.OrRd, alpha=0.3)
                for i in range(conf_matrix.shape[0]):
                    for j in range(conf_matrix.shape[1]):
                        ax.text(x=j, y=i,s=conf_matrix[i, j], va='center', ha='center', size='xx-large')
 
                plt.xlabel('Predictions', fontsize=18)
                plt.ylabel('Actuals', fontsize=18)
                plt.title('Confusion Matrix', fontsize=18)
                plt.show()
                
                print(str(split) +"-"+ str(accuracy)+"-"+ str(precision)+"-"+ str(recall))

split = 0
for data in data_splits:
                X_train, X_test, Y_train, Y_test = data[0], data[1], data[2], data[3]
                
                apply_model(X_train, Y_train, X_test, Y_test, split)
                split += 1
           
"""
for data, grp_id in list(zip(data_splits, grp_ids)):
                X_train, X_test, Y_train, Y_test = data[0], data[1], data[2], data[3]
                apply_model(X_train, Y_train, X_test, Y_test, grp_id)"""

            
performance["overall_metrics"]["avg_precision"] = np.round(tot_precision / split, 2)
performance["overall_metrics"]["avg_recall"] = np.round(tot_recall / split, 2)
performance["overall_metrics"]["avg_accuracy"] = np.round(tot_acc / split, 2)
print(performance)
