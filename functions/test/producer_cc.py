"""
Reads JSON with Conrol Center and produces the message

"""

from confluent_kafka import Producer
import json
from datetime import datetime


# Define Kafka broker settings
KAFKA_BROKER = "apps.edutel.uniwa.gr:9092"  # Change if your broker is different
KAFKA_TOPIC =  "CommandControl" #"CommandControl"   # Change to your topic name


def delivery_report(err, msg):
    """ Callback for message delivery reports. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def update_timestamp(message):
    """ Update the sentUTC field in the JSON message with the current timestamp. """
    current_time = datetime.utcnow().isoformat() + "Z"  # Get current UTC time in ISO format
    try:
        message["records"][0]["value"]["header"]["sentUTC"] = current_time
        print(f"Updated sentUTC: {current_time}")
    except KeyError as e:
        print(f"Error: Missing key {e} in JSON data")
    
    return message

def main():
    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'security.protocol': 'PLAINTEXT',
        'client.id': 'colab-producer'
    }

    # Create a Kafka producer
    producer = Producer(producer_config)



    #read json message
    with open('msg_cc_inside.json') as f:
        data = f.read() 

    message = json.loads(data)
    message = update_timestamp(message)


    message = json.dumps(message, indent=2).encode('utf-8')
    # Send messages

    # message = f"Hello Kafka! Message {i}"
    producer.produce(KAFKA_TOPIC, message,key="key", callback=delivery_report)
    producer.poll(0)  # Ensure the callback gets triggered
    producer.flush()

    print("Messages sent successfully!")


if __name__ == '__main__':
    main()
