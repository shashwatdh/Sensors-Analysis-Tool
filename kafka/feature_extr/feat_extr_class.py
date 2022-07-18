import paho.mqtt.client as mqtt
import ssl
import os
import json
import numpy as np
import pandas as pd
from functools import reduce
import sys
from os.path import exists
from services import fetch_time_stmp, df_cols
from missingDataImputer import missing_data_impute_exp
from feat_extr_module import *
import time
from kafka import KafkaConsumer
from kafka import KafkaProducer


class feature_extraction:
    def __init__(self, sensors_config) :
        #self.sensors_config = os.path.join("..", "sensors_config.json")
        with open(sensors_config) as fp:
            self.config_data = json.load(fp)
        fp.close()

        self.feat_ext_config = self.config_data["feature_extraction"]
        self.feat_ext_preprocessing_config = self.config_data["feature_extraction"]["pre_processing"]
        self.feat_ext_types = list(self.config_data["feature_extraction"]["types"].keys())

        self.file_name = "feat_ext_" + fetch_time_stmp() + ".csv"
        self.file_path = os.path.join(".", self.file_name)
        self.file_rec_buf = []
        self.recs_buf_limit = self.feat_ext_config["file_handling"]["rec_buf_limit"]
        self.rec_counter = 0

        self.subTopic = "merge_node_feat_ext"
        self.consumer = KafkaConsumer(self.subTopic, bootstrap_servers='localhost:9092')

        self.abort_proc = False

    def file_dump(self):
        #global rec_counter
        #global file_rec_buf
        feat_extr_df = pd.DataFrame(self.file_rec_buf)
        feat_extr_df.to_csv(self.file_path, mode="a", header=False)
        self.rec_counter = 0
        self.file_rec_buf = []

    def process_data_packet(self, payload):       
        """
            features = []
            # For each type of feat_extr, pass its config along with data
            for feat_ext_type in feat_ext_types:
            #for feat in list(feat_ext_config[feat_ext_type].keys()):
            if feat_ext_type == "statistical":   
            features.extend(extract_statistical_feat(payload, feat_ext_config[feat_ext_type]))"""
        #global rec_counter
        #global file_rec_buf
        #global abort_proc
        #print(payload)
        #merged_data = pd.DataFrame(payload)
        rcvd_merged_data = json.loads(payload)
        print("len of data rcvd:",len(rcvd_merged_data["data"]))
    
        if len(rcvd_merged_data["data"]) == 0:#rcvd_merged_data["data"] == "end":
            print("Aborting the feature extraction module")
            self.abort_proc = True
            return 
       
        else:
            print("Normal exec")
        
        merged_data = rcvd_merged_data["data"][:]
        merged_df = pd.DataFrame(merged_data, columns=rcvd_merged_data["header"])
    
        # Perform Preprocessing steps
        # Handle missing data values
    
        # for now remove rows with missing data
        handle_missing_rows = self.feat_ext_preprocessing_config["data_cleaning"]["missing_data"]
        if handle_missing_rows == "discard":
            merged_df.dropna(axis=0, how="any", inplace=True)
        else:
            pass
    
        # Remove redundant entries
        # normalization or standarisation
    
        # feature extraction
        features = [rcvd_merged_data["data"][0][0]]
        header = [rcvd_merged_data["header"][0]]
        for feat_ext_type in self.feat_ext_types:
            #for feat in list(feat_ext_config[feat_ext_type].keys()):
            if feat_ext_type == "statistical":   
                #features.extend(extract_statistical_feat(merged_df, feat_ext_config["types"][feat_ext_type]))
                extr_features, extr_header = extract_statistical_feat(merged_df, self.feat_ext_config["types"][feat_ext_type])
                features.extend(extr_features)
                if not exists(self.file_path):
                    header.extend(extr_header)
    
        if not exists(self.file_path):
            pd.DataFrame([header]).to_csv(self.file_path, mode="a", header=False)
        
        print("feat extr:",features)
        self.file_rec_buf.append(features[:])
        self.rec_counter += 1
        if self.rec_counter == self.recs_buf_limit:
            # push recs to file
            self.file_dump()
            
            
        # Define on connect event function
        # We shall subscribe to our Topic in this function
        """def on_connect(mosq, obj, flags,rc):
            mqttc.subscribe(MQTT_TOPIC, 1)"""

        # Define on_message event function. 
        # This function will be invoked every time,
        # a new message arrives for the subscribed topic 

    def msgHandler(self):
        for message in self.consumer:
            #msg = json.loads(message.value.decode("utf-8"))
            self.process_data_packet(message.value.decode("utf-8"))
            #print(json.loads(msg)["order_id"])
            if self.abort_proc:
                if self.rec_counter: #len(file_rec_buf):
                    self.file_dump()
                break


