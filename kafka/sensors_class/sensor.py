import numpy as np
import pandas as pd
import math 
import paho.mqtt.client as mqtt
import ssl
import sys
import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
import threading
class sensor:
    def __init__(self, config_data):
    
        # Initializing sensor related params 
        self.sensor_id = config_data["id"]
        self.dataset_path = config_data["dataset_path"]
        self.reqd_cols = config_data["dataset_reqd_cols"]
        self.window_size = config_data['window_size']
        self.step_size = math.ceil(self.window_size * config_data["step_size"])
        self.rows_limit = config_data["rows_limit"]
        self.chunk_size = config_data["chunk_size"]
        self.comm_channel = config_data["comm_channel"]
        
        self.pub_topic = config_data["pub_topic"]
        self.sub_topic = config_data["sub_topic"]
        self.abort_proc = False
        self.thread_started = False

        # Initializing AWS_IoT related params
        if self.comm_channel == "AWS_IoT":
            self.AWS_endpoint = config_data["AWS_endpoint"]
            self.AWS_port = config_data["AWS_port"]
            self.CA_path = config_data["CA_path"]
            self.certf_path = config_data["certf_path"] 
            self.key_path = config_data["key_path"]
            #self.pub_topic = config_data["pub_topic"]
            #self.sub_topic = config_data["sub_topic"]
        
        elif self.comm_channel == "kafka":
            self.consumer = KafkaConsumer(self.sub_topic, bootstrap_servers='localhost:9092')
            self.producer = KafkaProducer(bootstrap_servers="localhost:9092")
            
            self.t1 = threading.Thread(target=self.__bck_thread)
            self.t1.start()
            #self.__bck_thread()
            
        self.disconnected = False
        
        self.base_TS = -1
        self.window_data = {
                            "sensor_id" : self.sensor_id,
                            "base_Timestamp" : -1,
                            "data" : []
                            }
        
        self.dataset_read_completed = False
        self.error = ""
        self.chunk_count = 0
        self.current_chunk = []
        self.current_chunk_index = 0 # indicates cur row in chunk
        
        self.publish_data_lock = False #True
        self.data_list = []
        self.dataset = pd.read_csv(self.dataset_path, chunksize=self.chunk_size) 
        self.sensor_data = []
        """ 
            data_windows_list maintains segmented windows of raw data.
            When senor node requests for a segmented window, if data_windows_list is empty
            then a new window is extracted for node. Else, a segmented window is fetched 
            from list.
            
        """
        self.data_windows_list = []  
        
        # Initialize connection with Broker if comm_channel is AWS_IoT
        self.mqtt_client = None
        if self.comm_channel == "AWS_IoT" :
            self.mqtt_client = mqtt.Client(self.sensor_id)
            self.mqtt_client.on_message = self.__on_message
            self.mqtt_client.on_connect = self.__on_connect
            self.mqtt_client.on_subscribe = self.__on_subscribe
            self.mqtt_client.on_publish = self.__on_publish
            
            self.mqtt_client.tls_set(ca_certs=self.CA_path, certfile=self.certf_path, keyfile=self.key_path, cert_reqs=ssl.CERT_REQUIRED,
                          tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)       # pass parameters
             
            self.mqtt_client.connect(self.AWS_endpoint, self.AWS_port, keepalive=60)
            self.mqtt_client.loop_start()

    def __bck_thread(self):
        #global lock
        #global producer
        #global abort_proc
        #global thread_started
        #global data_list
        #global consumer

        #consumer = KafkaConsumer(sub_topic, bootstrap_servers='localhost:9092')
        print("sensor thread started....")
        self.thread_started = True

        for msg in self.consumer:
               
            r_msg = json.loads(msg.value.decode("utf-8"))
            #print(json.loads(msg)["order_id"])
            print(r_msg["status"])
            if r_msg["status"] == 403:
                self.abort_proc = True
                self.disconnected = True
                self.data_list = []
                break
        
            else:
                print("rcvd ack - ", r_msg["status"])
                print("packet BS (to be sent):", self.data_list[0]["base_Timestamp"])
                self.__push_data_pck()
                #data_pck = data_list.pop(0) # Remove head packet
                #producer.send("order_details", data_pck) #json.dumps(data_pck).encode("utf-8"))
                print("packet sent...")



    def __on_message(self, mosq, obj, msg):
        #global publish_data_lock
        print("Topic: " + str(msg.topic))
        
        """
            Data ACK is rcvd. 
            
            If END packet has already been pushed to queue, then
            from data queue push head packet on data channel.
            
            Else, just disbale the lock for producer to push 
            data on channel.
        """
        res = json.loads(msg.payload)
        if res["status"] == 403:
            # terminate the process
            print("Disconnecting.....")
            self.mqtt_client.unsubscribe(self.sub_topic)
            self.mqtt_client.loop_stop()    #Stop loop 
            self.mqtt_client.disconnect() # disconnect
            self.disconnected = True
            sys.exit(1)
        else:
            print(res["status"])
            
        if len(self.data_list) and (self.data_list[-1]['data'] == "End"):  
            self.__push_data_pck()
        
        else:
            self.publish_data_lock = False
            
    def __on_connect(self, client, userdata, flags, rc):
        #global publish_data_lock                                                 
        if rc:
            print("Connection refused")
            sys.exit(1)
            return
        print("Connection to AWS is successful")
        #self.mqtt_client.loop_start()
        # Subscribe to ACK topic
        self.mqtt_client.subscribe(self.sub_topic, 1)
        
        if len(self.data_list) and (self.data_list[-1]['data'] == "End"):  
            self.__push_data_pck()
            
        self.publish_data_lock = False           # Enable producer to publish data on channel
        
    def __on_subscribe(self, mosq, obj, mid, granted_qos):
        print("Subscribed to Topic: " + self.sub_topic)
        
    def __on_publish(self,client,userdata,result):
        print("Data Published")
    
    def __push_data_pck(self):
        data_pck = self.data_list.pop(0) # Remove head packet
        if self.comm_channel == "AWS_IoT":
            self.mqtt_client.publish(self.pub_topic, json.dumps(data_pck), qos=1)
        else:
            self.producer.send(self.pub_topic, json.dumps(data_pck).encode("utf-8"))
        print("pub_Data:", data_pck, self.pub_topic)
        #pass
    
    def push_to_dataQ(self, sensor_data):
        #global publish_data_lock
        # Pushes sensor data to data queue
        self.window_data["base_Timestamp"] = sensor_data["base_Timestamp"]#sensor_data[0][0] #self.base_TS
        self.window_data["data"] = sensor_data["data"]
        self.data_list.append(self.window_data.copy())
        
        # check if producer can publish data
        if not self.publish_data_lock:
            self.__push_data_pck()
            self.publish_data_lock = True
            
    def __push_to_fetched_windQ(self, sensor_data):
        data = self.window_data
        data["base_Timestamp"] = self.base_TS#sensor_data[0][0] #self.base_TS
        data["data"] = sensor_data
        print(data)
        #self.data_list.append(self.window_data.copy())
        #self.data_windows_list.append(sensor_data)
        self.data_windows_list.append(data.copy())
    
    def fetch_cur_window_and_slide(self):
        
        """
            Fetches current window which needs to be sent/processed and slides the window
            Current window is list of lists
            
            Steps:
            - check if new chunk has to be extracted
            - 
        """
        
        # Check if window can be fetched from list 
        if len(self.data_windows_list):
            response = self.data_windows_list.pop(0)
            print("Window fetched from queue:", response)
            return response
        
        if self.dataset_read_completed and len(self.sensor_data):
            print("last pkt:",{"sensor_id":self.sensor_id, "base_Timestamp": self.base_TS, "data" : self.sensor_data[:]})
            return {"sensor_id":self.sensor_id, "base_Timestamp": self.base_TS, "data" : self.sensor_data[:]}
        
        if len(self.current_chunk) == 0:
            try:
                self.current_chunk = self.dataset.get_chunk(self.chunk_size).iloc[:,self.reqd_cols].values  #.iloc[:self.rows_limit, self.reqd_cols].values
            
            except StopIteration:
                self.dataset_read_completed = True
                return[]
            
            except Exception as e:
                print("Error:",e.__class__)
                self.dataset_read_completed = True
                self.disconnected = True
                self.error = "Error:" + e.__class__
                # clear the var
                return []
                #sys.exit(1)
            
            #if len(self.current_chunk) < self.chunk_size:
            #    self.dataset_read_completed = True
            
            self.chunk_count += 1
            self.current_chunk_index = 0
            
            if self.base_TS < 0:
                self.base_TS = (self.current_chunk[0][0] // self.window_size) * self.window_size
        
        row = self.current_chunk_index
        cur_chunk = self.current_chunk
        baseTS = self.base_TS
        window_size = self.window_size
        
        while cur_chunk[row][0] < (baseTS + window_size):
            self.sensor_data.append(cur_chunk[row][:].tolist())
            #sensor_data.append(rec[:].tolist()) 
            if ((self.chunk_count - 1) * self.chunk_size) + (row + 1) == self.rows_limit:
                self.dataset_read_completed = True
                break
            
            row = (row + 1) % len(self.current_chunk)
            if not row:
                try:
                    self.current_chunk = self.dataset.get_chunk(self.chunk_size).iloc[:,self.reqd_cols].values #.iloc[:self.rows_limit, self.reqd_cols].values
                    cur_chunk = self.current_chunk
                    self.chunk_count += 1
                except StopIteration:
                    self.dataset_read_completed = True
                    break
                    #return
                
                except Exception as e:
                    print("Error:",e.__class__)
                    sys.exit(1)
            
            #if ((self.chunk_count - 1) * self.chunk_size) + (row + 1) == self.rows_limit:
            #    self.dataset_read_completed = True
        
        
        '''
        Record belongs to different window. So first append populated data
        to data queue.
        
        Set base_TS value and clear window_data. Add new entry in 
        window_data.
        '''
        response = {"sensor_id":self.sensor_id,"base_Timestamp" : baseTS ,"data" : self.sensor_data[:]}
        self.current_chunk_index = row
        """
        if len(self.sensor_data):
            #self.data_windows_list.append(self.sensor_data)
            self.__push_to_fetched_windQ(self.sensor_data[:])
        """    
        
        baseTS += self.step_size
        self.base_TS = baseTS
        
        while len(self.sensor_data) and self.sensor_data[0][0] < baseTS:
            del self.sensor_data[0]
        
        #-----------------------------
        '''
        # from cur window remove entries having timestamp < updated base_TS
        while len(self.sensor_data) and self.sensor_data[0][0] < baseTS:
            del self.sensor_data[0]
        #sensor_data = [rec for rec in sensor_data if rec[0] >= base_TS]         
        # Check if row can be added in slided window
        if cur_chunk[row][0] >= (baseTS + window_size):           #rec[0] >= (base_TS + window_size):
            """ 
                cur rec can't be added to window, so push cur sensor data to
                data queue. And set cur rec's TS as base TS
            
            """
            if len(self.sensor_data):
                #self.data_windows_list.append(self.sensor_data)
                print("data added to q:")
                self.__push_to_fetched_windQ(self.sensor_data[:])  # this is only true for 50%overlap
            
            self.sensor_data = []
            """
                base_TS can't be assigned to some random value. We must calculate
                base_TS of window nearest to rec if no error would have occured. 
            """
            baseTS = (cur_chunk[row][0] // window_size) * window_size  #(rec[0] // window_size) * window_size
            self.base_TS = baseTS
            #base_TS += step_size if (base_TS + step_size) <= rec[0] else 0
        
        self.sensor_data.append(cur_chunk[row][:].tolist())         #(rec[:].tolist())
        self.current_chunk_index = (row + 1) % len(cur_chunk)
        if not self.current_chunk_index:
            self.current_chunk = []
        self.base_TS = baseTS
        print("res:", response)
        return response'''
           
    #-----------------------------------------------
        
        while(self.dataset_read_completed) or (cur_chunk[row][0] >= (baseTS + window_size)):
            #print(self.dataset_read_completed)
            
            if not len(self.sensor_data):
                break
            
            print("data added to queue:")
            self.__push_to_fetched_windQ(self.sensor_data[:])
            
            baseTS += self.step_size
            self.base_TS = baseTS
            
            # from cur window remove entries having timestamp < updated base_TS
            while len(self.sensor_data) and self.sensor_data[0][0] < baseTS:
                del self.sensor_data[0]
            
            
        if not self.dataset_read_completed:
            if not len(self.sensor_data):
                baseTS = (cur_chunk[row][0] // window_size) * window_size  #(rec[0] // window_size) * window_size
                self.base_TS = baseTS
            self.sensor_data.append(cur_chunk[row][:].tolist())
            if ((self.chunk_count - 1) * self.chunk_size) + (row + 1) == self.rows_limit:
                self.dataset_read_completed = True
                #break
            else:
                self.current_chunk_index = (row + 1) % len(cur_chunk)
                if not self.current_chunk_index:
                    self.current_chunk = []
                    
        print("res:", response)
        return response
    def send_data():
        pass