import threading
import queue
import time
from typing import Optional, Dict, Any
from shapely.geometry import Point, Polygon
from functions import display_tools as dt
from functions.ManageKafka import Consumer_UAV_Telemetry, Consumer_PathPlanning, KafkaProducer, Message
from functions.logger import setup_logger

logger = setup_logger()

class KafkaHandler:
    """
    A thread-safe handler for Kafka operations that manages:
    1. Asynchronous message consumption from Kafka topics
    2. Asynchronous message sending to Kafka topics
    3. Drone position tracking and polygon validation
    4. Metadata management for detections
    
    This class uses separate threads to prevent Kafka operations from blocking
    the main detection loop.
    """
    def __init__(self, broker: str, producer_topic: str, path_planning_topic: str, UAV_Telemetry_topic: str, selected_drone_id: int = 1,selected_drone_name: str = None):
        """
        Initialize the Kafka handler with configuration.
        
        Args:
            broker: Kafka broker address
            producer_topic: Topic for sending detection messages
            command_control_topic: Topic for receiving command control messages
            path_planning_topic: Topic for receiving path planning messages
            selected_drone_id: The drone ID to listen for in the telemetry topic
        """
        # Initialize Kafka clients with correct broker
        self.consumer_pp = Consumer_PathPlanning(broker=broker)
        self.consumer_telemetry = Consumer_UAV_Telemetry(broker=broker)
        self.kafka_producer = KafkaProducer(broker, producer_topic)
        
        # Set topics
        self.consumer_telemetry.topic_in = UAV_Telemetry_topic
        self.consumer_pp.topic_in = path_planning_topic
        
        # Queues for thread-safe communication between threads
        self.detection_queue = queue.Queue()  # Queue for detection messages to be sent
        
        # Thread control flags
        self.running = False
        self.last_message_time = time.time()
        self._latest_pp_message: Optional[Dict[str, Any]] = None
        self._latest_telemetry_message: Optional[Dict[str, Any]] = None
        self._pp_lock = threading.Lock()
        self._telemetry_lock = threading.Lock()

        
        # Current state - stores latest metadata from CommandControl
        self.current_metadata = {
            "batteryPercentage": "None",
            "uav_status": "down",
            "droneID": "None",
            "latitude": "0.00000000",
            "longitude": "0.00000000",
            "altitude": "0.00",
            "drone_name": "Unknown",
            "gimbalAngle": "None",
            "heading": "None"
        }
        
        # Thread instances
        self.consumer_thread = None
        self.sender_thread = None

        # Store selected drone ID
        self.selected_drone_id = selected_drone_id
        self.selected_drone_name = selected_drone_name

    def start(self):
        """
        Start the Kafka handler threads:
        - Consumer thread: Continuously polls for new messages
        - Sender thread: Processes detection messages from queue
        """
        # Start consumers
        self.consumer_telemetry.start()
        self.consumer_pp.start()
        
        # Start handler threads
        self.running = True
        self.consumer_thread = threading.Thread(target=self._consumer_loop)
        self.sender_thread = threading.Thread(target=self._sender_loop)
        
        self.consumer_thread.start()
        self.sender_thread.start()


    def stop(self):
        """
        Gracefully stop all Kafka handler threads and wait for them to finish.
        """
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join()
        if self.sender_thread:
            self.sender_thread.join()
            
        # Stop consumers
        self.consumer_telemetry.stop()
        self.consumer_pp.stop()
        
        dt.print_green("[KafkaHandler] Stopped all threads")

    def _consumer_loop(self):
        """
        Main loop for consuming Kafka messages.
        Continuously polls for new CommandControl messages and updates:
        - Current metadata
        - Drone position
        - Polygon validation status
        """
        dt.print_yellow("\n[KafkaHandler] Starting consumer loop")
        while self.running:
            try:
                # First check for path planning messages
                pp_message = self.consumer_pp.get_latest_message_pp()
                if pp_message:
                    # logger.info("[KafkaHandler] Received new path planning message")
                    with self._pp_lock:
                        self._latest_pp_message = pp_message
                
                # Then check UAV Telemetry messages
                message_telemetry = self.consumer_telemetry.get_latest_message_telemetry()
                print("\n🐍 kafka_handler.py | _consumer_loop ~ message_telemetry",message_telemetry)
                # print('\n')
                # logger.info(f"[KafkaHanlder] message_telemetry: {message_telemetry}")

                if message_telemetry:
                    if self.selected_drone_id is not None and message_telemetry.get("drone_id") != self.selected_drone_id:
                        continue
                    
                    if self.selected_drone_name is not None and message_telemetry.get("drone_name") != self.selected_drone_name:
                        continue

                    # with self._telemetry_lock:
                    #     self._latest_telemetry_message = message_telemetry
                    # dt.print_blue(f"[KafkaHandler] Received command control message: {message_cc}")
                    # self.last_message_time = time.time()
                    
                    self._update_metadata(message_telemetry)
                    
                     
            except Exception as e:
                dt.print_red(f"[KafkaHandler] Error in consumer loop: {e}")
                time.sleep(1)  # Wait before retrying


    def _sender_loop(self):
        """
        Main loop for sending Kafka messages.
        Processes detection messages from the queue and sends them to Kafka.
        """
        dt.print_blue("[KafkaHandler] Starting sender loop")
        while self.running:
            try:
                # Get detection data from queue
                detection_data = self.detection_queue.get(timeout=1)
                if detection_data:
                    # Send message to Kafka
                    self.kafka_producer.send_message(detection_data)
                    self.detection_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                dt.print_red(f"[KafkaHandler] Error in sender loop: {e}")
                

    def _update_metadata(self, message: Dict[str, Any]):
        """
        Update the current metadata with new message data.
        Handles conversion and formatting of values.
        """
        try:
            self.current_metadata = {
                "drone_name": message.get("drone_name"),
                "droneID": message.get("drone_id"),
                "uav_status": message.get("uav_status"),
                
                "latitude": message.get('latitude'),
                "longitude": message.get('longitude'),
                "altitude": message.get('altitude'),
                
                "gimbalAngle": message.get("gimbalAngle"),
                "heading": message.get("heading"),
                "batteryPercentage": message.get("batteryPercentage", "None")
            }
            print("\n🐍 kafka_handler | _update_metadata ~ current_metadata",self.current_metadata)
                        
        except (ValueError, TypeError) as e:
            dt.print_red(f"[KafkaHandler] Error updating metadata: {e}")

    def add_detection(self, detection_data: Dict[str, Any]):
        """
        Add detection data to the queue for sending.
        This is called by the ObjectDetector when new detections are available.
        """
        self.detection_queue.put(detection_data)

    def get_current_metadata(self) -> Dict[str, Any]:
        """
        Get a copy of the current metadata.
        Used by ObjectDetector to include metadata in detection messages.
        """
        
        return self.current_metadata.copy()

    def get_polygon(self) -> Optional[Polygon]:
        """
        Get the current polygon from the path planning consumer.
        """
        return self.consumer_pp.polygon

    def get_no_flight_zones(self) -> list:
        """
        Get the current no-flight zones from the path planning consumer.
        """
        return getattr(self.consumer_pp, "no_flight_zones", [])

    def get_latest_pp_message(self):
        """
        Get the latest message from the path planning consumer.
        Returns the message or None if no message is available.
        """
        with self._pp_lock:
            return self._latest_pp_message
    
    def get_latest_telemetry_message(self):
        """
        Get the latest message from the command control consumer.
        Returns the message or None if no message is available.
        """
        with self._telemetry_lock:
            return  self._latest_telemetry_message

    def is_drone_in_polygon(self) -> bool:
        """
        Check if the drone is inside the polygon and not in any NFZ.
        This is the main validation method used by ObjectDetector to determine
        if detection should proceed.
        """
        try:
            # Get current drone position
            lon = float(self.current_metadata["longitude"])
            lat = float(self.current_metadata["latitude"])
            drone_point = Point(lon, lat)
            # print("\n🐍 kafka_handler |is_drone_in_polygon ~ drone_point",drone_point)
            
            # dt.print_red(f'[Handler-in polygon] drone_point: {drone_point}')

            # Get polygon and NFZs
            polygon = self.get_polygon()
            no_flight_zones = self.get_no_flight_zones()


            if not polygon:
                dt.print_blue("[KafkaHandler] No polygon defined")
                return False
            
            # Check if drone is in polygon
            if not polygon.contains(drone_point):
                # logger.info("[KafkaHandler] Drone is outside polygon")
                return False
            
            # Check if drone is in any NFZ
            for i, nfz in enumerate(no_flight_zones):
                if nfz.contains(drone_point):
                    logger.info(f"[KafkaHandler] Drone in NFZ {i+1}")
                    # dt.print_blue("[KafkaHandler] Drone is in no-flight zone")
                    return False
            
            return True
            
        except Exception as e:
            dt.print_red(f"[KafkaHandler] Error checking drone position: {e}")
            return False 