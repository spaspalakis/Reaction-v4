from confluent_kafka import Consumer, KafkaException

# Kafka Configuration
conf = {
    'bootstrap.servers': 'apps.edutel.uniwa.gr:9092',
    'security.protocol': 'PLAINTEXT',  
    'group.id': 'ode-consumer',
    'auto.offset.reset': 'latest'  #
}

# Create Kafka Consumer
consumer = Consumer(conf)
topic_in = "UAV_Telemetry" #'CommandControl'   # Listen to this topic

# Subscribe to the topic
consumer.subscribe([topic_in])

print(f"\nListening on topic: {topic_in}")

try:
    while True:
        msg = consumer.poll(1.0)  # Poll for messages every second
        print("[]")

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
