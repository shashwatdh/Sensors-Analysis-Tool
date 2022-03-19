import time as t
import json
import AWSIoTPythonSDK.MQTTLib as AWSIoTPyMQTT
import os

# Define ENDPOINT, CLIENT_ID, PATH_TO_CERTIFICATE, PATH_TO_PRIVATE_KEY, PATH_TO_AMAZON_ROOT_CA_1, MESSAGE, TOPIC, and RANGE
ENDPOINT = "a2jdem77nz5dot-ats.iot.us-east-1.amazonaws.com"
CLIENT_ID = "sensor_gyro"
#PATH_TO_CERTIFICATE = os.path.join(".", "certs","860a1b6d8f5e59344dad19c0e47189889c42600722987e93e3b20befb6dc9439-certificate.pem.crt")
PATH_TO_CERTIFICATE = os.path.join(".", "certf_gyro","sensor_gyro-certificate.pem.crt") 
#"C:\\Users\\shash\\Documents\\rp_sensor_tool\\AWS IoT\\certf_acc\\sensor_acc-certificate.pem.crt.txt"
#PATH_TO_PRIVATE_KEY = os.path.join(".", "certs","860a1b6d8f5e59344dad19c0e47189889c42600722987e93e3b20befb6dc9439-private.pem.key")
PATH_TO_PRIVATE_KEY = os.path.join(".", "certf_gyro","sensor_gyro-private.pem.key")
#PATH_TO_AMAZON_ROOT_CA_1 = os.path.join(".", "certs","AmazonRootCA1 (1).pem")
PATH_TO_AMAZON_ROOT_CA_1 = os.path.join(".", "certf_gyro","AmazonRootCA1(2).pem")

MESSAGE = "Hello World"
TOPIC = "test/testing"
RANGE = 20

myAWSIoTMQTTClient = AWSIoTPyMQTT.AWSIoTMQTTClient(CLIENT_ID)
myAWSIoTMQTTClient.configureEndpoint(ENDPOINT, 8883)
myAWSIoTMQTTClient.configureCredentials(PATH_TO_AMAZON_ROOT_CA_1, PATH_TO_PRIVATE_KEY, PATH_TO_CERTIFICATE)

myAWSIoTMQTTClient.connect()
print('Begin Publish')
for i in range (RANGE):
    data = "{} [{}]".format(MESSAGE, i+1)
    message = {"message" : data}
    myAWSIoTMQTTClient.publish(TOPIC, json.dumps(message), 1) 
    print("Published: '" + json.dumps(message) + "' to the topic: " + "'test/testing'")
    t.sleep(0.1)
print('Publish End')
myAWSIoTMQTTClient.disconnect()