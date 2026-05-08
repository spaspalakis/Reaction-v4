import json
import os
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
    msg_counter = 1 

    def __init__(self, droneID,drone_name,uav_status):       

        self.message = {
            "records": [
                {
                    "value": {
                        "header": {
                            "topicName": "ObjectDetection",
                            "msgIdentifier": str(Message.msg_counter), #str(random.randint(1000, 9999)),
                            "uav_status": uav_status,
                            "droneID": droneID,
                            "drone_name": drone_name,
                            "end_session": False,
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
        Message.msg_counter += 1 
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

    @staticmethod
    def build_end_session_message(drone_id, drone_name, uav_status=None):
        """Minimal ObjectDetection record signaling consumers that the OD session segment ended."""
        mid = str(Message.msg_counter)
        Message.msg_counter += 1
        header = {
            "topicName": "ObjectDetection",
            "msgIdentifier": mid,
            "droneID": drone_id,
            "drone_name": drone_name,
            "end_session": True,
            "sentUTC": datetime.utcnow().isoformat() + "Z",
        }
        if uav_status is not None:
            header["uav_status"] = uav_status
        return {"records": [{"value": {"header": header}}]}


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
        self.kafka_log_every_n_frames = 20


    def delivery_report(self,err, msg):
        """ Callback for message delivery reports. """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
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
        header = message['records'][0]['value']['header']
        mid = header['msgIdentifier']
        message_bytes = json.dumps(message, indent=2).encode('utf-8')
        self.producer.produce(self.topic, message_bytes, key="key", callback=self.delivery_report)
        self.producer.poll(0)
        self.producer.flush()

        # Keep terminal cleaner: print Kafka send status every N frames.
        frame_ids = []
        body = header.get("body", {})
        detection_list = body.get("detection_list", []) if isinstance(body, dict) else []
        for item in detection_list:
            if isinstance(item, dict) and item.get("frameID") is not None:
                try:
                    frame_ids.append(int(item["frameID"]))
                except (TypeError, ValueError):
                    pass

        if frame_ids:
            max_frame_id = max(frame_ids)
            if max_frame_id % self.kafka_log_every_n_frames == 0:
                logger.info(f"[Kafka] Sent detection in frame {max_frame_id} (msgIdentifier={mid})")
        elif header.get("end_session", False):
            logger.info(f"[Kafka] End-session message sent (msgIdentifier={mid})")



class Consumer_PathPlanning:
    def __init__(self, broker='apps.edutel.uniwa.gr:9092'):
        self.conf = {
            'bootstrap.servers': broker,
            'security.protocol': 'PLAINTEXT',
            'group.id': f'1ode-v2-pp-consumer',
            'enable.auto.commit' : True,
            'auto.offset.reset': 'latest'
        }
        self.topic_in = 'PathPlanning_Input'
        self.consumer = Consumer(self.conf)
        self.polygon = None
        self.no_flight_zones = []

    def start(self):
        """Initialize the Kafka consumer and subscribe to the topic."""
        try:
            self.consumer.subscribe([self.topic_in])
        except Exception as e:
            logger.error(f"[Consumer_PP] Failed to subscribe to topic {self.topic_in}: {e}")

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
                logger.info("[Consumer_PP] Main polygon received")

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
    def __init__(self, broker='apps.edutel.uniwa.gr:9092',selected_drone_id=None, selected_drone_name=None):
        self.conf = {
            'bootstrap.servers': broker,
            'security.protocol': 'PLAINTEXT',
            'group.id': f'ode-v2-telemetry-consumer-{uuid.uuid4()}',
            'enable.auto.commit': True,
            'auto.offset.reset': 'latest'
        }
        self.topic_in = 'UAV_Telemetry'
        self.consumer = Consumer(self.conf)
        self.selected_drone_id = selected_drone_id
        self.selected_drone_name = selected_drone_name



    def start(self):
        """Initialize the Kafka consumer and subscribe to the topic."""
        try:
            self.consumer.subscribe([self.topic_in])
        except Exception as e:
            logger.error(f"[Consumer_Telemetry] Failed to subscribe to topic {self.topic_in}: {e}")


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
            
            # telemetry = message.get("telemetry", {})
            if not message:
                print("[Consumer_Telemetry] Error: No telemetry in message")

            if self.selected_drone_name is not None and message["drone_name"] != self.selected_drone_name:
                return None
            if self.selected_drone_id is not None and message["drone_id"] != self.selected_drone_id:
                return None
                            
            # dt.print_green(f"\n[ManageKafka] result: {result}")
            return message

        except Exception as e:
            print(f"[Consumer_Telemetry] Error in get_latest_message: {e}")
            return None

    def stop(self):
        """Cleanly close the consumer."""
        self.consumer.close()
        print("\n[Consumer_Telemetry] Stopped.")

