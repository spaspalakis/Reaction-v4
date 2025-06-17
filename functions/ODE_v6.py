import os
import cv2 as cv
import time,json,requests
import numpy as np
import tensorflow as tf
from tqdm import tqdm
from confluent_kafka import Producer
from pprint import pprint
from shapely.geometry import Point
from collections import deque

# Tracker and detection modules
from deep_sort_utils.deep_sort_misc import create_box_encoder, Detection
from deep_sort_utils import tracker as tracker_module
from deep_sort_utils import nn_matching
from compute_overlap import compute_overlap


# Utility functions
from functions import display_tools as dt
from functions import preprocess_image as pp_im
from functions import nms_tf as nms
from functions import filter_duplicates
from functions import create_final_track_list
from functions import display_bboxes_cv3 as dspl_bboxes_cv3
from functions import create_video_webM as webm
from functions import ManageKafka
from functions.internet_utils import get_external_ip
from functions.kafka_handler import KafkaHandler
from functions.logger import setup_logger   
logger = setup_logger()

FONT_SIZE = 14
OVERLAP_THRESHOLD = 0.5

class ObjectDetector:
    def __init__(self, config, kafka_handler):
        """Initialize the object detector with the given configuration."""
        self.config = config
        self.kafka_handler = kafka_handler
        
        self.encoder = create_box_encoder(config['encoder_path'], batch_size=32)
        self.metric = nn_matching.NearestNeighborDistanceMetric("cosine", config['max_cosine_distance'], config['nn_budget'])
        
        self.tracker = tracker_module.Tracker(self.metric)
        self.class_names = [c.strip() for c in open(config['labels_path']).readlines()]
        self.classes_index = {x: y for (x, y) in enumerate(self.class_names)}
        
        self.ip = get_external_ip()
        
        self.video_writer = None
        self.video_open = False
        self.frame_data = []
        self.detection_map = {}



    def update_metadata(self, metadata):
        """
        Update the detector's metadata with new drone position and status.
        
        Args:
            metadata: Dictionary containing drone position and status information
        """
        self.kafka_handler.current_metadata.update({
            "uav_status": metadata.get("uav_status", "down"),
            "droneID": metadata.get("droneID", "None"),
            "missionID": metadata.get("missionID", "None"),
            "latitude": metadata.get("latitude", "0.00000000"),
            "longitude": metadata.get("longitude", "0.00000000"),
            "altitude": metadata.get("altitude", "0.00")
        })


    def save_json(self, batch_id):
        """Save detection data to JSON and send to Kafka."""
        json_path = os.path.join(self.config['json_folder'], f"reaction_batch_{batch_id}.json")
        
        # Create message object with current metadata
        message_obj = ManageKafka.Message(
            uav_status=self.kafka_handler.get_current_metadata()["uav_status"],
            droneID=self.kafka_handler.get_current_metadata()["droneID"],
            mission_id=self.kafka_handler.get_current_metadata()["missionID"],
            geolocation={
                "latitude": self.kafka_handler.get_current_metadata()["latitude"],
                "longitude": self.kafka_handler.get_current_metadata()["longitude"],
                "altitude": self.kafka_handler.get_current_metadata()["altitude"]
            },
        )
        
        # Add detections to the message
        self.finalize_detections(message_obj)
        
        # Convert to JSON
        message_json = message_obj.to_json()
        
        # Save locally
        with open(json_path, "w") as f:
            f.write(message_json)
        # print(f"\nJSON file saved: {json_path}")
        logger.info(f"JSON file saved: {json_path}")

        # Add detected data to Kafka queue for sending
        self.kafka_handler.add_detection(message_json)
        
        # Reset detection map for next batch
        self.detection_map = {}

    def add_detection(self, ip, frame_id, object_id, object_class, confidence, bbox):
        """Store detection under the correct frameID."""
        if frame_id not in self.detection_map:
            self.detection_map[frame_id] = {
                "frameID": frame_id,
                "imageURL": f"http://{ip}:8000/images/frame_{frame_id:04d}.jpg",
                "detections": [],
                "GeoLocation": {
                    "latitude": self.kafka_handler.get_current_metadata()["latitude"],
                    "longitude": self.kafka_handler.get_current_metadata()["longitude"],
                    "altitude": self.kafka_handler.get_current_metadata()["altitude"]
                }
            }
        
        self.detection_map[frame_id]["detections"].append({
            "objectID": object_id,
            "class": object_class,
            "confidence": confidence,
            "bbox": bbox
        })

    def finalize_detections(self, message_obj):
        """Convert stored detections into the correct JSON structure."""
        for frame_data in self.detection_map.values():
            message_obj.message["records"][0]["value"]["header"]["body"]["detection_list"].append(frame_data)

    def process_frame(self, image, infer, frame_id, ip, create_video):
        """Process a single frame and return annotated image and detected tracks."""

        original_image = image.copy()
        h_orig, w_orig, _ = original_image.shape
    
        image = pp_im.preprocess_image(image, self.config['model_input_size'])
        image_np_expanded = dt.cv(image)
        pred_bbox = infer(tf.constant(image_np_expanded))

        boxes, classes, scores, valid_detections = nms.nms_tf(
            pred_bbox, w_orig, h_orig,
            model_dim=self.config['model_input_size'],
            overlap_thres=self.config['overlap_thres'],
            score_thres=self.config['score_thres']
        )

        if len(boxes) > 0:
            
            if create_video:
                self.start_video_writer()

            # Deep SORT tracking
            boxes2 = boxes.copy().astype(int)
            boxes2[:, [2, 3]] = boxes2[:, [2, 3]] - boxes2[:, [0, 1]]

            # The order of features does not affect the order of tracks. So, a IoU comparison should be
            # performed on the returned bboxes
            features = self.encoder(original_image, boxes2)
            detections = [Detection(boxes2[i, :], scores[i], features[i, :]) for i in range(len(boxes2))]

            # Update tracker. 
            # This involves predicting the next position of existing tracks and updating them with the new detections.  
            self.tracker.predict()
            self.tracker.update(detections)

            # Track objects
            track_ids = [track.track_id for track in self.tracker.tracks if track.is_confirmed() and track.time_since_update <= 1]
            boxes2 = np.array([track.to_tlwh() for track in self.tracker.tracks if track.is_confirmed() and track.time_since_update <= 1])

            if len(track_ids) > 0:
                boxes2 = np.concatenate((boxes2[:, (0, 1)], boxes2[:, (0, 1)] + boxes2[:, (2, 3)]), axis=1).astype(np.float64)
                overlaps = compute_overlap(boxes.astype(np.float64), boxes2)
                max_overlaps_idx = np.argmax(overlaps, axis=1).astype(np.int32)
                max_overlaps_idx = filter_duplicates.filter_doubles(overlaps, max_overlaps_idx)

                final_track_ids = [-1] * len(boxes)
                for i, overlap_value in enumerate(overlaps[np.arange(len(overlaps)), max_overlaps_idx]):
                    if overlap_value > OVERLAP_THRESHOLD:
                        final_track_ids[i] = track_ids[max_overlaps_idx[i]]
                    else:
                        final_track_ids[i] = 0

                filtered_boxes, filtered_labels, filtered_scores, filtered_track_ids = create_final_track_list.filter_boxes(
                    final_track_ids, boxes, [self.classes_index[c] for c in classes], scores)

                
                annot_img = dspl_bboxes_cv3.display_bboxes(
                        original_image,
                        bboxes=filtered_boxes,
                        labels=filtered_labels,
                        title='fixed',
                        scores=filtered_scores,
                        tracks=filtered_track_ids,
                        font_size=FONT_SIZE)
                
                for i in range(len(filtered_boxes)):
                    self.add_detection(
                        ip=ip,
                        frame_id=frame_id,
                        object_id=filtered_track_ids[i],
                        object_class=filtered_labels[i],
                        confidence=float(filtered_scores[i]),
                        bbox=filtered_boxes[i].tolist()
                    )

                return annot_img
        
        else: # Handle empty detections
            return None
            

    def run(self, config, camera, infer, create_video, save_frames):
        """Main detection loop."""

        batches_sent = 0
        frame_times = []  # List to store processing times for each frame
        start_time = time.time()
        fr_count = 0
        detections_count = 0
        last_polygon_state = None  # Track the last known polygon state

        try:
            while camera.isOpened() and self.kafka_handler.running:
                frame_start_time = time.time()  # Start timing this frame
                
                ret, image = camera.read()
                if not ret:
                    logger.warning("\n[ODE] Video Stream ended... Exiting!")
                    break
                
              
                # # Check if drone is in polygon, if not skip frames
                # if not self.kafka_handler.is_drone_in_polygon():
                #     fr_count += 1
                #     continue
                
                output_image = self.process_frame(image, infer, fr_count, self.ip, create_video)
                
                # Skip if no detections
                if output_image is None:
                    fr_count += 1
                    time.sleep(0.1)  # Prevent busy waiting
                    continue
                
                 
                # Calculate frame processing time
                frame_time = time.time() - frame_start_time
                frame_times.append(frame_time)
                
                fr_count += 1
                detections_count += 1
                
                if detections_count % config['message_size'] == 0:
                    self.save_json(detections_count)
                    batches_sent += 1
                
                if save_frames:
                    im_path = f"{self.config['frames_folder']}/frame_{fr_count:04d}.jpg"
                    cv.imwrite(im_path, output_image)
                
            # # Print stats every 100 frames
            # if len(frame_times) >= 100:
            #     # Calculate average FPS over last 100 frames
            #     avg_frame_time = sum(frame_times[-100:]) / 100
            #     current_fps = 1.0 / avg_frame_time
                
            #     # Calculate overall FPS
            #     total_time = time.time() - start_time
            #     overall_fps = fr_count / total_time
                
            #     dt.print_magenta(f"\n[FPS Stats]")
            #     dt.print_magenta(f"Current FPS (last 100 frames): {current_fps:.2f}")
            #     dt.print_magenta(f"Overall FPS: {overall_fps:.2f}")
            #     dt.print_magenta(f"Processed: {fr_count} total frames")
            #     dt.print_magenta(f"Detections: {detections_count}")
            #     dt.print_magenta(f"Batches sent: {batches_sent}")
                
            #     # Keep only last 100 frame times to save memory
            #     frame_times = frame_times[-100:]

        except Exception as e:
            logger.error(f"[ODE] Error in detection loop: {str(e)}")
            raise
        finally:
            # Cleanup
            camera.release()
            logger.info("[ODE] Camera released")
        
        return True

    def stop_detection(self):
        """Stop detection and clean up resources."""
        # Stop video writer if it's open
        if self.video_writer is not None:
            self.video_writer.release()
            self.video_writer = None
            self.video_open = False
            print("\n[ObjectDetector] Video writer closed")
        
        # Clear any pending detections
        self.detection_map = {}
        self.frame_data = []
        
        # Reset tracker
        self.tracker = tracker_module.Tracker(self.metric)
        
        # print("\n[ObjectDetector] Detection stopped and resources cleaned up")


