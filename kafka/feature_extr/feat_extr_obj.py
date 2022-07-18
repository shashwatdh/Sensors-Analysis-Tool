#from pandas import merge
import feat_extr_class
import multiprocessing
import json
import os

def initialize_feat_extr_node(config_path):
    obj = feat_extr_class.feature_extraction(config_path)
    obj.msgHandler()
    #obj.connect2broker()

if __name__ == "__main__":
    sensors_config = os.path.join("..", "sensors_config.json")
    
    feat_extr_proc = multiprocessing.Process(target=initialize_feat_extr_node, args=(sensors_config,))

    feat_extr_proc.start()
    feat_extr_proc.join()