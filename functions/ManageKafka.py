import json
from pprint import pprint
from datetime import datetime
import random
from shapely.geometry import Polygon


from confluent_kafka import Producer
from confluent_kafka import Consumer
from functions import display_tools as dt



class Message:
    def __init__(self, uav_status, droneID,drone_name, geolocation):
        self.message = {
            "records": [
                {
                    "value": {
                        "header": {
                            "topicName": "ObjectDetection",
                            "msgIdentifier": str(random.randint(1000, 9999)),
                            "uav_status": uav_status,
                            "droneID": droneID,
                            "drone_name": drone_name,
                            "sentUTC": datetime.utcnow().isoformat() + "Z",
                            "district": "Athens",
                            "body": {
                                "detection_list": []
                            }
                        }
                    }
                }
            ]
        }
        # self.geolocation = geolocation
        self.detection_map = {}



    def add_detection(self, frame_id, object_id, object_class, confidence, bbox):
            if frame_id not in self.detection_map:
                self.detection_map[frame_id] = {
                    "frameID": frame_id,
                    "imageURL": "",
                    "detections": [],
                    # "GeoLocation": self.geolocation
                }
            
            self.detection_map[frame_id]["detections"].append({
                "objectID": object_id,
                "class": object_class,
                "confidence": confidence,
                "bbox": bbox
            })

    def finalize_detections(self):
        self.message["records"][0]["value"]["header"]["body"]["detection_list"] = list(self.detection_map.values())


    def to_json(self):
        return json.dumps(self.message, indent=4)



class KafkaProducer:
    def __init__(self,broker,topic):
        self.broker = broker
        self.topic = topic

        self.producer_conf = {
            'bootstrap.servers': self.broker,#"apps.edutel.uniwa.gr:9092", #broker
            'security.protocol': 'PLAINTEXT',
            'client.id': 'reaction-producer'
        }
        
        self.producer = Producer(self.producer_conf)


    def delivery_report(self,err, msg):
        """ Callback for message delivery reports. """
        if err is not None:
            print(f"Message delivery failed: {err}")
        # else:
            # print(f"[Producer] Message delivered to {msg.topic()} [{msg.partition()}]")

    @staticmethod
    def update_timestamp(message):
        """ Update the sentUTC field in the JSON message with the current timestamp. """
        current_time = datetime.utcnow().isoformat() + "Z"  # Get current UTC time in ISO format
        try:
            message["records"][0]["value"]["header"]["sentUTC"] = current_time
            # print(f"Updated sentUTC: {current_time}")
        except KeyError as e:
            print(f"Error: Missing key {e} in JSON data")
        
        return message


    def send_message(self,message):
        """ Sends a message to the Kafka topic. """
        if isinstance(message, str):
            message = json.loads(message)  # Convert string to dictionary if necessary

        message = self.update_timestamp(message)

        message_bytes = json.dumps(message, indent=2).encode('utf-8')
        self.producer.produce(self.topic, message_bytes, key="key", callback=self.delivery_report)
        self.producer.poll(0)
        self.producer.flush()
        print("Messages sent successfully!")



class Consumer_PathPlanning:
    def __init__(self, broker='apps.edutel.uniwa.gr:9092'):
        self.conf = {
            'bootstrap.servers': broker,
            'security.protocol': 'PLAINTEXT',
            'group.id': 'reaction-consumer',
            'auto.offset.reset': 'latest'
        }
        self.topic_in = 'PathPlanning_Input'
        print(f"\n[Consumer_PP] Initializing with broker: {broker}")
        self.consumer = Consumer(self.conf)
        self.polygon = None
        self.no_flight_zones = []

    def start(self):
        """Initialize the Kafka consumer and subscribe to the topic."""
        try:
            self.consumer.subscribe([self.topic_in])
            print(f"[Consumer_PP] Successfully subscribed to topic: {self.topic_in}")
        except Exception as e:
            print(f"[Consumer_PP] Error subscribing to topic: {e}")

    def get_latest_message(self):
        """
        Get the latest message from the Kafka topic.
        Returns a dictionary with UAV status and drone ID, or None if no message is received.
        """
        try:
            msg = self.consumer.poll(1.0)  # Poll for messages every second

            if msg is None:
                return None
            if msg.error():
                print(f"[Consumer_PP] Kafka error: {msg.error()}")
                return None

            try:
                message = json.loads(msg.value().decode('utf-8'))
                # print(f"\n[Consumer_PP] Received raw message: {message}")
                geometries = message['Geometries'][0]['geometry']['coordinates']

                # Main polygon (first)
                self.polygon = Polygon(geometries[0])
                print("\n[Consumer_PP] Main polygon updated: ", self.polygon)

                # No-flight zones (all others)
                self.no_flight_zones = []
                for nfz_coords in geometries[1:]:
                    nfz_polygon = Polygon(nfz_coords)
                    self.no_flight_zones.append(nfz_polygon)
                    print("\n[Consumer_PP] No-flight zone added:", nfz_polygon)

                return message

            except json.JSONDecodeError as e:
                print(f"[Consumer_PP] Error decoding message: {e}")
                return None
            except KeyError as e:
                print(f"[Consumer_PP] Missing key in message: {e}")
                return None
            except Exception as e:
                print(f"[Consumer_PP] Error processing message: {e}")
                return None

        except Exception as e:
            print(f"[Consumer_PP] Error in get_latest_message: {e}")
            return None

    def stop(self):
        """Cleanly close the consumer."""
        self.consumer.close()
        print("\n[Consumer] Stopped.")




class Consumer_UAV_Telemetry:
    def __init__(self, broker='apps.edutel.uniwa.gr:9092'):
        self.conf = {
            'bootstrap.servers': broker,
            'security.protocol': 'PLAINTEXT',
            'group.id': 'assaode-v2-consumer',
            'auto.offset.reset': 'latest'
        }
        self.topic_in = 'UAV_Telemetry'
        print(f"[Consumer_Telemetry] Initializing with broker: {broker}")
        self.consumer = Consumer(self.conf)

    def start(self):
        """Initialize the Kafka consumer and subscribe to the topic."""
        try:
            self.consumer.subscribe([self.topic_in])
            print(f"[Consumer_Telemetry] Successfully subscribed to topic: {self.topic_in}")
        except Exception as e:
            print(f"[Consumer_Telemetry] Error subscribing to topic: {e}")


    def get_latest_message(self):
        """
        Get the latest message from the UAV_Telemetry Kafka topic.
        Returns a dictionary with only the required fields and polygon check.
        """

        try:
            msg = self.consumer.poll(1.0)  # Poll for messages every second

            if msg is None:
                return None
            if msg.error():
                print(f"[Consumer_Telemetry] Kafka error: {msg.error()}")
                return None

            try:
                message = json.loads(msg.value().decode('utf-8'))
                telemetry = message.get("telemetry", {})
                result = {
                    "drone_name": message.get("drone_name", "unknown"),
                    "drone_id": message.get("drone_id", "unknown"),
                    "latitude": telemetry.get("latitude"),
                    "longitude": telemetry.get("longitude"),
                    "altitude": telemetry.get("altitude"),
                    "uav_status": telemetry.get("droneState"),
                    "gimbalAngle": telemetry.get("gimbalAngle"),
                    "heading": telemetry.get("heading")
                }
                
                return result

            except json.JSONDecodeError as e:
                print(f"[Consumer_Telemetry] Error decoding message: {e}")
                return None
            except KeyError as e:
                print(f"[Consumer_Telemetry] Missing key in message: {e}")
                return None
            except Exception as e:
                print(f"[Consumer_Telemetry] Error processing message: {e}")
                return None

        except Exception as e:
            print(f"[Consumer_Telemetry] Error in get_latest_message: {e}")
            return None

    def stop(self):
        """Cleanly close the consumer."""
        self.consumer.close()
        print("\n[Consumer_Telemetry] Stopped.")

