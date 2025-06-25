import json
from pprint import pprint
from datetime import datetime
import random
from shapely.geometry import Polygon


from confluent_kafka import Producer
from confluent_kafka import Consumer
from functions import display_tools as dt
import uuid # Add this import
from functions.logger import setup_logger
logger = setup_logger()



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
            'group.id': f'1ode-v2-pp-consumer',
            # 'enable_auto_commit' : True,
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

    def get_latest_message_pp(self):
        """
        Get the latest message from the Kafka topic.
        Returns a dictionary with UAV status and drone ID, or None if no message is received.
        """
        try:
            msg = self.consumer.poll(0.5)  # Poll for messages every second
            # print("🐍 File: functions/ManageKafka.py | Line: 146 | get_latest_message_pp ~ msg",msg)

            if msg is None:
                return None
            if msg.error():
                logger.info(f"[Consumer_PP] Kafka error: {msg.error()}")
                return None

            try:
                message = json.loads(msg.value().decode('utf-8'))
                # print(f"\n[Consumer_PP] Received raw message: {message}")
                geometries = message['Geometries'][0]['geometry']['coordinates']

                # Main polygon (first)
                self.polygon = Polygon(geometries[0])
                # print("\n[Consumer_PP] Main polygon updated: ", self.polygon)
                logger.info("\n[Consumer_PP] Main polygon received")

                # No-flight zones (all others)
                self.no_flight_zones = []
                for nfz_coords in geometries[1:]:
                    nfz_polygon = Polygon(nfz_coords)
                    self.no_flight_zones.append(nfz_polygon)
                    # print("\n[Consumer_PP] No-flight zone added:", nfz_polygon)
                    logger.info("[Consumer_PP] NFZ received")

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
            'group.id': f'ode-v2-telemetry-consumer-{uuid.uuid4()}',
            # 'enable.auto.commit': True,
            'auto.offset.reset': 'latest'
        }
        self.topic_in = 'UAV_Telemetry'
        print(f"[Consumer_Telemetry] Broker Initialized | groud-id: {self.conf['group.id']}")
        self.consumer = Consumer(self.conf)

    def start(self):
        """Initialize the Kafka consumer and subscribe to the topic."""
        try:
            self.consumer.subscribe([self.topic_in])
            print(f"[Consumer_Telemetry] Successfully subscribed to topic: {self.topic_in}")
        except Exception as e:
            print(f"[Consumer_Telemetry] Error subscribing to topic: {e}")


    def get_latest_message_telemetry(self):
        """
        Get the latest message from the UAV_Telemetry Kafka topic.
        Returns a dictionary with only the required fields and polygon check.
        """

        try:
            msg = self.consumer.poll(1.0)  # Poll for messages every second
           

            # print(f"\nmsg: {msg}")
            if msg is None:
                return None
            if msg.error():
                logger.info(f"[Consumer_Telemetry] Kafka error: {msg.error()}")
                return None

            # try:
            message = json.loads(msg.value().decode('utf-8'))
            # print("\n🐍 ManageKafka.py | get_latest_message_telemetry ~ message",message)
            
            telemetry = message.get("telemetry", {})
            if not telemetry:
                print("[Consumer_Telemetry] Error: No telemetry in message")

            result = {
                    "drone_name": message.get("drone_name", "unknown"),
                    "drone_id": message.get("drone_id", "unknown"),
                    "latitude": telemetry.get("latitude"),
                    "longitude": telemetry.get("longitude"),
                    "altitude": telemetry.get("altitude"),
                    "uav_status": telemetry.get("droneState"),
                    "gimbalAngle": telemetry.get("gimbalAngle"),
                    "heading": telemetry.get("heading"),
                    'batteryPercentage': telemetry.get("batteryPercentage")
                }
                
            # dt.print_green(f"\n[ManageKafka] result: {result}")
            return result

        except Exception as e:
            print(f"[Consumer_Telemetry] Error in get_latest_message: {e}")
            return None

    def stop(self):
        """Cleanly close the consumer."""
        self.consumer.close()
        print("\n[Consumer_Telemetry] Stopped.")

