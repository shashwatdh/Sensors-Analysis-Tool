#!/usr/bin/env python

# This script works as a Environmental Station, so it will compute (random) values
# through its virtual sensor and it will publish the message composed by the values
# calculated before on the MQTT channel, as long as the station works correctly.

# importing libraries
import paho.mqtt.client as mqtt
import ssl
from time import sleep
from random import randint
import json
import datetime
import os
connflag = False
 
def on_connect(client, userdata, flags, rc):                        
    global connflag                                                 
    print("Connection to AWS")
    connflag = True
    print("Connection returned result: " + str(rc) )
 
mqttc = mqtt.Client('Env_Sensor_1')                                 # MQTT client object                              
mqttc.on_connect = on_connect                                       # assign on_connect function

#### Change following parameters #### 
awshost = "a2jdem77nz5dot-ats.iot.us-east-1.amazonaws.com"                  # endpoint
awsport = 8883   
caPath = os.path.join(".", "certs_acc","AmazonRootCA1(2).pem")
certPath = os.path.join(".", "certs_acc","69b5f46c37d85bbd295373c5385b0f95382dd7765427f97facaeda4e20dd166e-certificate.pem.crt.txt")
keyPath = os.path.join(".", "certs_acc","69b5f46c37d85bbd295373c5385b0f95382dd7765427f97facaeda4e20dd166e-private.pem.key")                                                   # port no.   

mqttc.tls_set(ca_certs=caPath, certfile=certPath, keyfile=keyPath, cert_reqs=ssl.CERT_REQUIRED,
              tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)       # pass parameters
 
mqttc.connect(awshost, awsport, keepalive=60)                       # connect to AWS server
 
mqttc.loop_start()                                                  # start background network thread
                                                                    
while True:
    sleep(5)                                                        # waiting between messages
    if connflag == True:
        temp = str(randint(-50, 50))                                # computation of all the (random) values
        hum = str(randint(0, 100))                                  # of the sensors, for this
        wind_dir = str(randint(0, 360))                             # specific station (with id 1)
        wind_int = str(randint(0, 100))
        rain = str(randint(0, 50))
        time = str(datetime.datetime.now())[:19]                    
        
        data ={"deviceid":str(1), "datetime":time, "temperature":temp, "humidity":hum,
               "windDirection":wind_dir, "windIntensity":wind_int, "rainHeight":rain}
        jsonData = json.dumps(data)
        mqttc.publish("sensor/data", jsonData, qos=1)               # publish message 
      
        print("Message sent: time ",time)                           
        print("Message sent: temperature ",temp," Celsius")         
        print("Message sent: humidity ",hum," %")
        print("Message sent: windDirection ",wind_dir," Degrees")
        print("Message sent: windIntensity ",wind_int," m/s")
        print("Message sent: rainHeight ",rain," mm/h\n")
    else:
        print("waiting for connection...")