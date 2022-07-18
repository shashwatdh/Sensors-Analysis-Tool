#from pandas import merge
import merge_class
import multiprocessing
import json
import os

def initialize_merge_node(config_path):
    obj = merge_class.merge(config_path)
    obj.connect2broker()

if __name__ == "__main__":
    sensors_config = os.path.join("..", "..", "sensors_config.json")
    
    merge_proc = multiprocessing.Process(target=initialize_merge_node, args=(sensors_config,))

    merge_proc.start()
    merge_proc.join()