import json
import time
from confluent_kafka import Producer

# Kafka settings
KAFKA_BROKER = "apps.edutel.uniwa.gr:9092"
KAFKA_TOPIC = "UAV_Telemetry"

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

def main():
    producer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'security.protocol': 'PLAINTEXT',
        'client.id': 'telemetry-producer'
    }
    producer = Producer(producer_config)

    drone_name = "Test_2"
    drone_id = 555
    message_id_counter = 0  # Initialize the message ID counter

    count = 0
    while True:
        # end_session = False
        # if count == 30:  # Send end_session=True after 20 messages
        #     end_session = True
        message = {
            # "message_id": message_id_counter,
            "timestamp": time.time(),
            "drone_name": drone_name,
            "drone_id": drone_id,
            
            # "end_session": end_session,
            "telemetry": {
                "latitude": 37.9643345,
                "longitude": 23.768096999999997,
                "altitude": 129.94801330566406,
                "heading": 97.44,
                "velocity": 19.20062681907261,
                "gpsSignal": 0,
                "satelliteNumber": 10,
                "homeLatitude": 0,
                "homeLongitude": 0,
                "droneState": "Flying",
                "gimbalAngle": -90,
                "batteryPercentage": 94.0,
                "vtolState": "FW"
            }
        }

        payload = json.dumps(message).encode('utf-8')
        producer.produce(KAFKA_TOPIC, payload, key=str(drone_id), callback=delivery_report)
        producer.poll(0)
        producer.flush()
        print(f"Sent telemetry: {message}")
        time.sleep(1)  # Send every second
        count += 1
        message_id_counter += 1 

if __name__ == '__main__':
    main() 