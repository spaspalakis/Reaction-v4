import json
import time
import random
from confluent_kafka import Producer
from shapely.geometry import Point, Polygon
import numpy as np

KAFKA_BROKER = "apps.edutel.uniwa.gr:9092"
TOPIC_PP = "PathPlanning_Input"
TOPIC_TELEMETRY = "UAV_Telemetry"

class ComplexPolygonProducer:
    def __init__(self, config):
        self.config = config
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BROKER,
            'client.id': 'complex-polygon-producer'
        })
        
        # Load polygon data
        with open('complex-polygon.json', 'r') as f:
            self.mission_data = json.load(f)
        
        # Extract main polygon and no-flight zone
        self.main_polygon = Polygon(self.mission_data['Geometries'][0]['geometry']['coordinates'][0])
        self.no_flight_zone = Polygon(self.mission_data['Geometries'][0]['geometry']['coordinates'][1])
        print(f"\nNFZ: {self.no_flight_zone}")
        
        # Define movement states with their durations (in seconds)
        self.states = {
            'outside': 10,  # 10 seconds outside
            'inside': 10,   # 10 seconds inside
            'no_flight': 10,  # 10 seconds in no-flight zone
            'inside_again': 10,  # 10 seconds inside again
            'outside_final': 10  # 10 seconds outside again
        }
        self.current_state = 0
        self.state_names = list(self.states.keys())
        
        # Calculate number of points based on duration (2 seconds between points)
        self.points_per_state = {state: duration // 2 for state, duration in self.states.items()}
        
        # Define movement points for each state
        self.movement_points = {
            'outside': self._generate_points_outside(self.points_per_state['outside']),
            'inside': self._generate_points_inside(self.points_per_state['inside']),
            'no_flight': self._generate_points_no_flight(self.points_per_state['no_flight']),
            'inside_again': self._generate_points_inside(self.points_per_state['inside_again']),
            'outside_final': self._generate_points_outside(self.points_per_state['outside_final'])
        }
        
        self.current_point_index = 0
        self.drone_id = "drone-001"
        self.mission_id = self.mission_data['MissionId']
        self.altitude = self.mission_data['properties']['altitude']
        
        # Print initial setup information
        print("\nInitializing Complex Polygon Producer:")
        print(f"Main polygon bounds: {self.main_polygon.bounds}")
        print(f"No-flight zone bounds: {self.no_flight_zone.bounds}")
        print(f"Mission ID: {self.mission_id}")
        print(f"Altitude: {self.altitude}m")
        print("\nState durations:")
        for state, duration in self.states.items():
            print(f"- {state}: {duration} seconds ({self.points_per_state[state]} points)")

    def send_path_planning(self):
        """Send the polygon and no-flight zone data to PathPlanning topic"""
        # Create the path planning message
        pp_message = {
            "MissionName": self.mission_data["MissionName"],
            "MissionId": self.mission_id,
            "Description": self.mission_data["Description"],
            "MissionType": self.mission_data["MissionType"],
            "Issuer": self.mission_data["Issuer"],
            "properties": self.mission_data["properties"],
            "Route": [],
            "Geometries": self.mission_data["Geometries"]
        }
        
        # Send to PathPlanning topic
        self.producer.produce(
            TOPIC_PP,
            key="path_planning",
            value=json.dumps(pp_message)
        )
        self.producer.flush()
        print("\nSent Path Planning message with polygon and no-flight zone data")
        time.sleep(2)  # Wait for 2 seconds to ensure message is processed

    def _generate_points_outside(self, num_points):
        """Generate points outside the main polygon"""
        bounds = self.main_polygon.bounds
        points = []
        # Generate points in a larger area around the polygon
        for _ in range(num_points):
            while True:
                # Generate points in a wider area around the polygon
                x = random.uniform(bounds[0] - 0.002, bounds[2] + 0.002)
                y = random.uniform(bounds[1] - 0.002, bounds[3] + 0.002)
                point = Point(x, y)
                if not self.main_polygon.contains(point):
                    points.append((x, y))
                    break
        return points

    def _generate_points_inside(self, num_points):
        """Generate points inside the main polygon but outside no-flight zone"""
        bounds = self.main_polygon.bounds
        points = []
        for _ in range(num_points):
            while True:
                x = random.uniform(bounds[0], bounds[2])
                y = random.uniform(bounds[1], bounds[3])
                point = Point(x, y)
                if self.main_polygon.contains(point) and not self.no_flight_zone.contains(point):
                    points.append((x, y))
                    break
        return points

    def _generate_points_no_flight(self, num_points):
        """Generate points inside the no-flight zone"""
        bounds = self.no_flight_zone.bounds
        points = []
        for _ in range(num_points):
            while True:
                x = random.uniform(bounds[0], bounds[2])
                y = random.uniform(bounds[1], bounds[3])
                point = Point(x, y)
                if self.no_flight_zone.contains(point):
                    points.append((x, y))
                    break
        return points

    def _get_location_status(self, point):
        """Get the status of a point relative to the polygons"""
        if self.no_flight_zone.contains(point):
            return "NO-FLIGHT ZONE"
        elif self.main_polygon.contains(point):
            return "INSIDE MAIN POLYGON"
        else:
            return "OUTSIDE MAIN POLYGON"

    def send_drone_position(self, longitude, latitude):
        """Send a message to Kafka with the current drone position"""
        point = Point(longitude, latitude)
        location_status = self._get_location_status(point)
        
        # Create message in the correct format
        drone_name = "Test"
        drone_id = 2
        message = {
            "drone_name": drone_name,
            "drone_id": drone_id,
            "timestamp": time.time(),
            "telemetry": {
                "latitude": latitude,
                "longitude": longitude,
                "altitude": self.altitude,
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
        
        self.producer.produce(
            TOPIC_TELEMETRY,
            key=self.drone_id,
            value=json.dumps(message)
        )
        self.producer.flush()
        print(f"\nDrone Position: ({longitude:.8f}, {latitude:.8f})")
        print(f"Status: {location_status}")
        print(f"State: {self.state_names[self.current_state]}")
        print(f"Time remaining in state: {self.states[self.state_names[self.current_state]] - (self.current_point_index * 2)} seconds")

    def start_detector(self):
        if not hasattr(self, '_detector_active') or not self._detector_active:
            print("\n[DETECTOR] Detector started: Reading frames from camera...")
            self._detector_active = True

    def stop_detector(self):
        if hasattr(self, '_detector_active') and self._detector_active:
            print("\n[DETECTOR] Detector stopped: Stopped reading frames from camera.")
            self._detector_active = False

    def run(self):
        """Run the producer simulation"""
        print("\nStarting complex polygon producer simulation...")
        # First, send the path planning data
        print("\nStep 1: Sending Path Planning data...")
        self.send_path_planning()
        # Wait for 10 seconds to ensure path planning is processed
        print("Waiting 10 seconds for path planning to be processed...")
        time.sleep(10)
        # Then start the drone movement simulation
        print("\nStep 2: Starting drone movement simulation...")
        print("Drone will follow this sequence:")
        print("1. Start outside main polygon (10 seconds)")
        print("2. Move inside main polygon (10 seconds)")
        print("3. Enter no-flight zone (10 seconds)")
        print("4. Return inside main polygon (10 seconds)")
        print("5. Move outside again (10 seconds)\n")

        self._detector_active = False
        prev_state = None
        while self.current_state < len(self.states):
            current_state = self.state_names[self.current_state]
            points = self.movement_points[current_state]

            # Detector logic: start/stop on entering/leaving inside states
            if current_state in ["inside", "inside_again"] and not self._detector_active:
                self.start_detector()
            elif current_state not in ["inside", "inside_again"] and self._detector_active:
                self.stop_detector()

            if self.current_point_index >= len(points):
                self.current_state += 1
                self.current_point_index = 0
                if self.current_state >= len(self.states):
                    break
                print(f"\nTransitioning to state: {self.state_names[self.current_state]}")
                continue

            longitude, latitude = points[self.current_point_index]
            self.send_drone_position(longitude, latitude)
            self.current_point_index += 1
            time.sleep(2)  # Wait 2 seconds between messages

        # Ensure detector is stopped at the end
        if self._detector_active:
            self.stop_detector()
        print("\nSimulation completed!")

if __name__ == "__main__":
    config = {
        'bootstrap.servers': KAFKA_BROKER,
        'security.protocol': 'PLAINTEXT',
        'client.id': 'complex-polygon'
    }
    
    producer = ComplexPolygonProducer(config)
    producer.run() 