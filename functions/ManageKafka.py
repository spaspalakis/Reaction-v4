import json
from pprint import pprint
from datetime import datetime
import random
from shapely.geometry import Polygon


from confluent_kafka import Producer
from confluent_kafka import Consumer
from functions import display_tools as dt



class Message:
    def __init__(self, uav_status, droneID, mission_id, geolocation):
        self.message = {
            "records": [
                {
                    "value": {
                        "header": {
                            "topicName": "ObjectDetection",
                            "msgIdentifier": str(random.randint(1000, 9999)),
                            "uav_status": uav_status,
                            "droneID": droneID,
                            "sentUTC": datetime.utcnow().isoformat() + "Z",
                            "district": "Athens",
                            "body": {
                                "missionID": mission_id,
                                "attachments": [],
                                "detection_list": []
                            }
                        }
                    }
                }
            ]
        }
        # self.geolocation = geolocation
        self.detection_map = {}



    def add_attachment(self, name, attachment_type, format, fps, height, width, url):
        attachment = {
            "attachmentName": name,
            "attachmentType": attachment_type,
            "attachmentFormat": format,
            "attachmentFPS": fps,
            "attachmentHeight": height,
            "attachmentWidth": width,
            "attachmentURL": url
        }
        self.message["records"][0]["value"]["header"]["body"]["attachments"].append(attachment)


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




class Consumer_CommandControl:
    def __init__(self, broker='apps.edutel.uniwa.gr:9092'):
        self.conf = {
            'bootstrap.servers': broker,
            'security.protocol': 'PLAINTEXT',
            'group.id': 'reaction-consumer',
            'auto.offset.reset': 'latest'
        }
        self.topic_in = 'CommandControl'
        print(f"[Consumer_CC] Initializing with broker: {broker}")
        self.consumer = Consumer(self.conf)

    def start(self):
        """Initialize the Kafka consumer and subscribe to the topic."""
        try:
            self.consumer.subscribe([self.topic_in])
            print(f"[Consumer_CC] Successfully subscribed to topic: {self.topic_in}")
        except Exception as e:
            print(f"[Consumer_CC] Error subscribing to topic: {e}")

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
                print(f"[Consumer_CC] Kafka error: {msg.error()}")
                return None

            try:
                message = json.loads(msg.value().decode('utf-8'))
                # print(f"\n[Consumer_CC] Received raw message: {message}")

                records = message.get("records", [])

                if not records:
                    print("[Consumer_CC] No records found in message")
                    return None

                value = records[0].get("value", {})
                header = value.get("header", {})
                body = header.get("body", {})  # Extract body safely

                # Extract header properly
                uav_status = header.get("uav_status", "down")  # default value down
                droneID = header.get("droneID", "unknown")
                missionID = body.get("missionID", "unknown")

                # Get geolocation directly from the GeoLocation field
                geolocation = body.get("GeoLocation", {})
                
                result = {
                    "uav_status": uav_status, 
                    "droneID": droneID,
                    "missionID": missionID,
                    "longitude": geolocation.get("longitude"),
                    "latitude": geolocation.get("latitude"),
                    "altitude": geolocation.get("altitude")
                }
                # dt.print_magenta(f"[Consumer_CC] Processed message: {result}")
                return result

            except json.JSONDecodeError as e:
                print(f"[Consumer_CC] Error decoding message: {e}")
                return None
            except KeyError as e:
                print(f"[Consumer_CC] Missing key in message: {e}")
                return None
            except Exception as e:
                print(f"[Consumer_CC] Error processing message: {e}")
                return None

        except Exception as e:
            print(f"[Consumer_CC] Error in get_latest_message: {e}")
            return None

    def stop(self):
        """Cleanly close the consumer."""
        self.consumer.close()
        print("\n[Consumer_CC] Stopped.")

