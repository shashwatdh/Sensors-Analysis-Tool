# Import package
import paho.mqtt.client as mqtt
import ssl
import os
import json

# Define Variables
MQTT_PORT = 8883
MQTT_KEEPALIVE_INTERVAL = 45
MQTT_TOPIC = "sensor/data"
MQTT_MSG = "hello MQTT"

awshost = "a2jdem77nz5dot-ats.iot.us-east-1.amazonaws.com"                  # endpoint
awsport = 8883   
"""
caPath = os.path.join(".", "certs_acc","AmazonRootCA1(2).pem")
certPath = os.path.join(".", "certs_acc","69b5f46c37d85bbd295373c5385b0f95382dd7765427f97facaeda4e20dd166e-certificate.pem.crt.txt")
keyPath = os.path.join(".", "certs_acc","69b5f46c37d85bbd295373c5385b0f95382dd7765427f97facaeda4e20dd166e-private.pem.key")                                                   # port no.   
"""

caPath = os.path.join(".", "certs_merge","AmazonRootCA1(3).pem")
certPath = os.path.join(".", "certs_merge","d056be5e3680c2fe06a1410774e479c33a3462e9e407c548f956031578c86e19-certificate.pem.crt.txt")
keyPath = os.path.join(".", "certs_merge","d056be5e3680c2fe06a1410774e479c33a3462e9e407c548f956031578c86e19-private.pem.key")                                                   # port no.   


MQTT_HOST = "put your Custom Endpoint here"
CA_ROOT_CERT_FILE = "put AWS IoT Root Certificate File Name here"
THING_CERT_FILE = "put your Thing's Certificate File Name here"
THING_PRIVATE_KEY = "put your Thing's Private Key File Name here"

# Define on connect event function
# We shall subscribe to our Topic in this function
def on_connect(mosq, obj, flags,rc):
    mqttc.subscribe(MQTT_TOPIC, 0)

# Define on_message event function. 
# This function will be invoked every time,
# a new message arrives for the subscribed topic 
def on_message(mosq, obj, msg):
    print("Topic: " + str(msg.topic))
    print("QoS: " + str(msg.qos))
	#print("Payload: " + str(msg.payload))
    data = json.loads(msg.payload)
    print(data)

def on_subscribe(mosq, obj, mid, granted_qos):
    print("Subscribed to Topic: " + 
	MQTT_MSG + " with QoS: " + str(granted_qos))
    print("Mid : " + str(mid))
# Initiate MQTT Client
mqttc = mqtt.Client()

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe

# Configure TLS Set
#mqttc.tls_set(caPath, certfile=certPath, keyfile=keyPath, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
mqttc.tls_set(ca_certs=caPath, certfile=certPath, keyfile=keyPath, cert_reqs=ssl.CERT_REQUIRED,
              tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None) 

# Connect with MQTT Broker
#mqttc.connect(awshost, awsport, MQTT_KEEPALIVE_INTERVAL)
mqttc.connect(awshost, awsport, keepalive=60)

# Continue monitoring the incoming messages for subscribed topic
mqttc.loop_forever()