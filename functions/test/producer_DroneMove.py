import json
import random
import time
from datetime import datetime
import copy

from confluent_kafka import Producer
from shapely.geometry import Point, Polygon

# Define Kafka broker settings
KAFKA_BROKER = "apps.edutel.uniwa.gr:9092"
KAFKA_TOPIC = "CommandControl"

# Define polygon as list of (lon, lat) tuples
POLYGON_COORDS = [
    (23.74843805859978, 37.97638404818811),
    (23.79031809636623, 37.97381302651111),
    (23.7678333219916, 37.96339270089696),
    (23.74843805859978, 37.97638404818811)
]
POLYGON = Polygon(POLYGON_COORDS)

# Define coordinates for different positions
OUTSIDE_POSITION_1 = (23.73, 37.97)  # West of polygon
INSIDE_POSITION = (23.76, 37.97)     # Center of polygon
OUTSIDE_POSITION_2 = (23.80, 37.97)  # East of polygon

def delivery_report(err, msg):
    """ Callback for message delivery reports. """
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

def update_timestamp(message):
    """ Update sentUTC timestamp. """
    current_time = datetime.utcnow().isoformat() + "Z"
    try:
        message["records"][0]["value"]["header"]["sentUTC"] = current_time
    except KeyError as e:
        print(f"Missing key {e}")
    return message

def update_geolocation(message, position):
    """ Update the message with specific geolocation. """
    lon, lat = position  # Unpack as (longitude, latitude)
    print(f"📍 Current geolocation: lat={lat:.6f}, lon={lon:.6f}")

    try:
        message["records"][0]["value"]["header"]["body"]["GeoLocation"] = {
            "latitude": f"{lat:.8f}",
            "longitude": f"{lon:.8f}",
            "altitude": "150.00"  # Example static altitude
        }
    except KeyError as e:
        print(f"Missing key {e}")
    return message

def main():
    # Kafka producer config
    producer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'security.protocol': 'PLAINTEXT',
        'client.id': 'geo-pattern-simulator'
    }

    producer = Producer(producer_config)

    # Load base message
    with open('msg_cc_inside.json') as f:
        base_message = json.load(f)

    # Define the pattern sequence with verified coordinates
    pattern = [
        (OUTSIDE_POSITION_1, 20),  # Outside for 10 seconds
        (INSIDE_POSITION, 60),     # Inside for 15 seconds
        (OUTSIDE_POSITION_2, 50)   # Outside for 50 seconds then stop
    ]

    try:
        # Iterate through the pattern
        for position, duration in pattern:
            print(f"\n🔄 Moving to new position for {duration} seconds...")
            start_time = time.time()
            
            while time.time() - start_time < duration:
                msg = copy.deepcopy(base_message)
                msg = update_timestamp(msg)
                msg = update_geolocation(msg, position)

                payload = json.dumps(msg, indent=2).encode('utf-8')
                producer.produce(KAFKA_TOPIC, payload, key="key", callback=delivery_report)
                producer.poll(0)
                producer.flush()

                time.sleep(1)  # Send message every second

    except KeyboardInterrupt:
        print("\n🛑 Pattern simulation stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        producer.flush()
        print("\n✅ Simulation completed")

if __name__ == '__main__':
    main()