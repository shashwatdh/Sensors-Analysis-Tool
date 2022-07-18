from kafka import KafkaConsumer
from kafka import KafkaProducer
import json 

consumer = KafkaConsumer('order_details', bootstrap_servers='localhost:9092')
producer = KafkaProducer(bootstrap_servers="localhost:9092")

def send_ack(sensor_id, status):
    # Sends ack
    res_data = {"status" : status}
    print(res_data)
    #mqttc.publish(publish_topics[sensor_id], json.dumps(res_data), qos=1)
    print("client" + str(sensor_id))
    producer.send("client1", json.dumps(res_data).encode("utf-8"))
    
print("Gonna start listening")
while True:
    for message in consumer:
        print("Here is a message..")
        print (message.value)
        """assert isinstance(msg.value, dict)
        msg = message
        print(message.order_id)
        print(message.items)
        print("\n")"""
        msg = message.value.decode("utf-8")
        #print(json.loads(msg)["order_id"])
        
        msg = json.loads(msg)
        if msg["order_id"] == 10:
            send_ack(1, 403)
        else:
            send_ack(1,200)            