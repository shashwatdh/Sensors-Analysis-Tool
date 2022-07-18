import json
import time
import threading
import sys

from kafka import KafkaProducer
from kafka import KafkaConsumer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 15
sub_topic = "client1"

producer = KafkaProducer(bootstrap_servers="localhost:9092")
consumer = KafkaConsumer(sub_topic, bootstrap_servers='localhost:9092')

abort_proc = False
data_list = []
lock = False #True
thread_started = False

print("Going to be generating order after 10 seconds")
print("Will generate one unique order every 5 seconds")
time.sleep(10)

def bck_thread():
    global lock
    global producer
    global abort_proc
    global thread_started
    global data_list
    global consumer
    #consumer = KafkaConsumer(sub_topic, bootstrap_servers='localhost:9092')
    print("thread started....")
    thread_started = True
    for msg in consumer:
        #if not thread_started:
            
            
        r_msg = json.loads(msg.value.decode("utf-8"))
        #print(json.loads(msg)["order_id"])
        print(r_msg["status"])
        if r_msg["status"] == 403:
            abort_proc = True
            data_list = []
            break
        
        else:
            print("rcvd ack - ", r_msg["status"])
            data_pck = data_list.pop(0) # Remove head packet
            producer.send("order_details", data_pck) #json.dumps(data_pck).encode("utf-8"))
            print(f"Done Sending..{i}")

t1 = threading.Thread(target=bck_thread)
t1.start()

while not thread_started:
    continue

time.sleep(1)
for i in range(ORDER_LIMIT):
    if abort_proc:
        #sys.exit()
        break
    data = {
        "order_id": i,
        "user_id": f"tom_{i}",
        "total_cost": i,
        "items": "burger,sandwich",
    }
    
    if not lock:
        producer.send("order_details", json.dumps(data).encode("utf-8"))
        print(f"Done Sending..{i}")
        lock = True
    else:
        print("Pushing data to list")
        data_list.append(json.dumps(data).encode("utf-8"))
        #time.sleep(5)

t1.join()