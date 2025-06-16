import json
from pprint import pprint 

from confluent_kafka import Consumer

def kafka_listener(stop_event, status_queue):
    conf = {
        'bootstrap.servers': 'apps.edutel.uniwa.gr:9092',
        'security.protocol': 'PLAINTEXT',
        'group.id': 'ode-consumer',
        'auto.offset.reset': 'latest'
    }

    consumer = Consumer(conf)
    topic_in = 'CommandControl'
    consumer.subscribe([topic_in])

    print(f"\nListening on topic: {topic_in}")

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)  # Poll for messages every second
                    
            if  msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                message = json.loads(msg.value().decode('utf-8'))
                records = message.get("records", [])

                #Extract header properly
                value = records[0].get("value", {})
                header = value.get("header", {})
                status = header.get("uav_status", "pending")  # default value pending
                droneID = header.get("droneID", "unknown")


                # pprint(f"\n2.\nmsg: {msg}\nmsg.value: {msg.value()}")
                # Put the new status and drone ID in the queue
                status_queue.put((status, droneID))
                print(f"\n[Kafka Listener] Status Updated: {status}, Drone ID: {droneID}")
                
                # Clear the queue before adding a new message to avoid stale data
                while not status_queue.empty():
                    status_queue.get()
                
                # Print only if the status changes
                # if status != uav_status[0]:
                #     uav_status[0] = status                    
                #     print(f"\n[Kafka Listener] UAV Status Changed: {uav_status[0]}")
                    

            except Exception as e:
                print(f"[Kafka Listener] Error processing message: {e}")
    finally:
        consumer.close()

    return 