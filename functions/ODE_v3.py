import os
import cv2 as cv
import time,json,requests
import base64
import numpy as np
import tensorflow as tf
from tqdm import tqdm
from confluent_kafka import Producer
from pprint import pprint
from shapely.geometry import Point
from collections import deque
from datetime import datetime

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
from functions import ManageKafka
from functions.internet_utils import get_external_ip
from functions.kafka_handler import KafkaHandler
from functions.logger import setup_logger   
from functions.obj_geolocation import pixel_to_gps
from functions.stream_diagnostics import StreamDiagnostics

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
        self._video_writer_failed = False
        self.video_open = False
        self.frame_data = []
        self.detection_map = {}
        self._current_frame_image = None



    def emit_detection_batch(self, frame_id, telemetry_msg, save_json_local: bool):
        """Build detection payload, publish to Kafka always; write JSON under json_folder only if save_json_local."""
        json_path = os.path.join(self.config['json_folder'], f"reaction_msg_{frame_id:04d}.json")

        telemetry = telemetry_msg.get("telemetry", {})

        message_obj = ManageKafka.Message(
            droneID=telemetry_msg.get("drone_id"),
            drone_name=telemetry_msg.get("drone_name"),
            uav_status=telemetry.get("droneState"),
        )
        verbose_message_logs = bool(self.config.get("verbose_message_logs", False))
        if verbose_message_logs and not __import__("os").environ.get("REACTION_QUIET", "").lower() in ("1", "true", "yes"):
            print(f"\n[ODE] Created message at {datetime.utcnow().isoformat()}Z: msgIdentifier={message_obj.message['records'][0]['value']['header']['msgIdentifier']}")

        self.finalize_detections(message_obj)
        message_json = message_obj.to_json()

        if save_json_local:
            with open(json_path, "w") as f:
                f.write(message_json)
            logger.info(f"[ODE] JSON file saved: {json_path}")

        self.kafka_handler.add_detection(message_json)
        self.detection_map = {}



    def add_detection(self,img_size,telemetry_msg, ip, frame_id, object_id, object_class, confidence, bbox):
        """Store detection under the correct frameID."""
        
        telemetry = telemetry_msg.get("telemetry", {})
        # dt.print_green(f'\n[ODE-add_detection] telemetry: {telemetry}'  )

        lat = telemetry.get("latitude") 
        lon = telemetry.get("longitude") 
        alt = telemetry.get("altitude") 

        if frame_id not in self.detection_map:
            image_b64 = ""
            if self._current_frame_image is not None:
                quality = self.config.get('image_jpeg_quality', 80)
                _, buf = cv.imencode('.jpg', self._current_frame_image, [cv.IMWRITE_JPEG_QUALITY, quality])
                image_b64 = base64.b64encode(buf).decode('utf-8')

            self.detection_map[frame_id] = {
                "frameID": frame_id,
                "imageData": image_b64,
                "detections": [],
                "GeoLocation": {
                    "latitude": lat,
                    "longitude": lon, 
                    "altitude": alt 
                }
            }
        fov = (68, 40)


        heading = telemetry.get("heading") 
        pitch =  telemetry.get("gimbalAngle") 
        pitch+=90

        drone_info = (lat, lon, alt, heading, pitch)
        
        # Calculate center pixel of bbox
        x1, y1, x2, y2 = bbox    
        center_pixel = (int((x1 + x2) / 2), int((y1 + y2) / 2))
        obj_gps = pixel_to_gps(center_pixel, img_size, fov, drone_info)

        self.detection_map[frame_id]["detections"].append({
            "objectID": object_id,
            "class": object_class,
            "confidence": confidence,
            "bbox": bbox,
            "obj_geolocation": obj_gps
        })

    def finalize_detections(self, message_obj):
        """Convert stored detections into the correct JSON structure."""
        for frame_data in self.detection_map.values():
            message_obj.message["records"][0]["value"]["header"]["body"]["detection_list"].append(frame_data)

    def process_frame(self, image, infer, frame_id, ip,img_size,telemetry_msg):
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
                
                self._current_frame_image = annot_img

                for i in range(len(filtered_boxes)):
                    if self.config.get("verbose", False):
                        print(
                            "[Verbose] "
                            f"frame={frame_id} track_id={filtered_track_ids[i]} "
                            f"class={filtered_labels[i]} confidence={float(filtered_scores[i]):.3f}"
                        )
                    self.add_detection(
                        img_size, 
                        telemetry_msg,
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
            

    def run(
        self,
        img_size,
        config,
        camera,
        infer,
        save_frames,
        save_json,
        polygon_flag,
        save_video=False,
        source_path=None,
        input_mode=None,
    ):
        """Main detection loop."""
        
        # In the run() method, after processing each frame:
        self.kafka_handler.increment_frame_count()
        
        batches_sent = 0
        frame_times = []  # List to store processing times for each frame
        fr_count = 0
        detections_count = 0
        total_detected_objects = 0
        no_detection_streak = 0
        no_detection_notice_every = int(
            config.get(
                "no_detection_periodic_message_every_frames",
                config.get("no_detection_notice_every_frames", 100),
            )
        )
        fps_frame_count = 0
        fps_start_time = time.time()

        vid_fps = float(camera.get(cv.CAP_PROP_FPS) or 0.0)
        if vid_fps <= 0.0 or np.isnan(vid_fps):
            vid_fps = 25.0

        max_consecutive_read_failures = int(config.get("max_read_failures", 30))
        reconnect_retry_delay_s = float(config.get("stream_retry_delay_s", 0.5))
        max_reopen_attempts = int(config.get("stream_reopen_attempts", 2))
        consecutive_read_failures = 0
        stream_stats_every_n_reads = int(
            config.get("stream_test_periodic_message_every_reads", config.get("stream_stats_every_n_reads", 100))
        )
        stream_test_file = config.get("stream_test_file")
        stream_test_enabled = bool(config.get("stream_test_enabled", False))

        frame_count_hint = camera.get(cv.CAP_PROP_FRAME_COUNT)
        is_likely_file = bool(frame_count_hint and frame_count_hint > 0 and np.isfinite(frame_count_hint))
        is_stream_input = input_mode in ("rtsp", "usb") or not is_likely_file

        stream_diag = StreamDiagnostics(
            enabled=stream_test_enabled,
            path=stream_test_file,
            stats_every_n_reads=stream_stats_every_n_reads,
        )

        video_out_path = None
        if save_video:
            video_out_path = os.path.join(
                config["video_output_folder"],
                config.get("video_output_filename", "output.mp4"),
            )

        was_in_detection_zone = False
        run_start_time = time.time()
        stream_diag.start(source_path=source_path, input_mode=input_mode, stream_mode=is_stream_input)
        try:
            while camera.isOpened() and self.kafka_handler.running:
                stream_diag.read_attempt()
                ret, image = camera.read()
                if not ret:
                    consecutive_read_failures += 1
                    logger.warning(
                        "[ODE] Frame read failed "
                        f"(count={consecutive_read_failures}/{max_consecutive_read_failures}, "
                        f"stream_mode={is_stream_input}, camera_open={camera.isOpened()})"
                    )
                    stream_diag.read_failure(
                        frame_idx=fr_count,
                        consecutive=consecutive_read_failures,
                        max_consecutive=max_consecutive_read_failures,
                    )

                    # For file inputs, EOF is expected: exit promptly.
                    if is_likely_file:
                        logger.warning("[ODE] Video file reached end or frame read failed. Exiting detection loop.")
                        stream_diag.file_end_or_fail()
                        if was_in_detection_zone:
                            self.kafka_handler.send_end_session_signal()
                            was_in_detection_zone = False
                        camera.release()
                        logger.info("[ODE] Camera released")
                        return False

                    # For streams (RTSP/USB), tolerate transient decode/read failures.
                    if is_stream_input and consecutive_read_failures < max_consecutive_read_failures:
                        reopened = False
                        if source_path:
                            for attempt in range(1, max_reopen_attempts + 1):
                                logger.warning(
                                    f"[ODE] Reopen attempt {attempt}/{max_reopen_attempts} for source: {source_path}"
                                )
                                stream_diag.reopen_attempt(attempt=attempt, max_attempts=max_reopen_attempts)
                                camera.release()
                                time.sleep(reconnect_retry_delay_s)
                                camera = cv.VideoCapture(source_path)
                                if camera.isOpened():
                                    reopened = True
                                    logger.info("[ODE] Stream reopened successfully")
                                    stream_diag.reopen_success()
                                    break
                        else:
                            time.sleep(reconnect_retry_delay_s)

                        if reopened or camera.isOpened():
                            continue

                    logger.warning("\n[ODE] Video Stream ended... Exiting!")
                    stream_diag.stream_end()
                    if was_in_detection_zone:
                        self.kafka_handler.send_end_session_signal()
                        was_in_detection_zone = False
                    camera.release()
                    logger.info("[ODE] Camera released")
                    return False  # Indicate video ended
                else:
                    if consecutive_read_failures > 0:
                        logger.info(f"[ODE] Stream recovered after {consecutive_read_failures} read failures")
                    stream_diag.read_success(recovered_after=consecutive_read_failures)
                    consecutive_read_failures = 0
                
                # # Update FPS counters
                # fps_frame_count += 1
                # current_time = time.time()
                # if current_time - fps_start_time >= 1.0:  # Every second
                #     fps = fps_frame_count / (current_time - fps_start_time)
                #     logger.info(f"[ODE] FPS: {fps:.2f} (processed {fps_frame_count} frames in last second)")
                #     fps_frame_count = 0
                #     fps_start_time = current_time

                # logger.info(f"[ODE] fr: {fr_count}")
                
                # If drone not in polygon skip frames (detection paused, not torn down)
                in_zone = self.kafka_handler.is_drone_in_polygon(polygon_flag)
                if was_in_detection_zone and not in_zone:
                    self.kafka_handler.send_end_session_signal()
                was_in_detection_zone = in_zone
                if not in_zone:
                    # dt.print_red('\n[ODE] Check inside polygon from ODE')
                    fr_count += 1
                    continue
                
                telemetry_msg = self.kafka_handler.get_latest_telemetry_message()

                current_frame_id = fr_count
                output_image = self.process_frame(image, infer, current_frame_id, self.ip, img_size, telemetry_msg)
                
                # Skip if no detections
                if output_image is None:
                    no_detection_streak += 1
                    if no_detection_notice_every > 0 and no_detection_streak % no_detection_notice_every == 0:
                        logger.info(
                            f"[ODE] Running normally, no detections for {no_detection_streak} consecutive frames"
                        )
                    fr_count += 1
                    time.sleep(0.1)  # Prevent busy waiting
                    continue
                
                fr_count += 1
                detections_count += 1
                if output_image is not None:
                    total_detected_objects += len(self.detection_map.get(current_frame_id, {}).get("detections", []))
                no_detection_streak = 0
                
                if detections_count % config['message_size'] == 0:
                    self.emit_detection_batch(current_frame_id, telemetry_msg, save_json)
                    batches_sent += 1
                    time.sleep(0.2)

                if save_frames:
                    im_path = f"{self.config['frames_folder']}/frame_{current_frame_id:04d}.jpg"
                    cv.imwrite(im_path, output_image)

                if save_video and video_out_path is not None:
                    if self.video_writer is None and not self._video_writer_failed:
                        h, w = output_image.shape[:2]
                        fourcc = cv.VideoWriter_fourcc(*"mp4v")
                        self.video_writer = cv.VideoWriter(
                            video_out_path, fourcc, vid_fps, (w, h)
                        )
                        if not self.video_writer.isOpened():
                            logger.error(
                                f"[ODE] VideoWriter failed to open: {video_out_path}"
                            )
                            self.video_writer = None
                            self._video_writer_failed = True
                    if self.video_writer is not None:
                        self.video_writer.write(output_image)

        except Exception as e:
            logger.error(f"[ODE] Error in detection loop: {str(e)}")
            raise
        finally:
            total_runtime_sec = time.time() - run_start_time
            stream_diag.end(
                total_detected_objects=total_detected_objects,
                total_runtime_sec=total_runtime_sec,
            )
            if was_in_detection_zone:
                self.kafka_handler.send_end_session_signal()
            if self.video_writer is not None:
                self.video_writer.release()
                self.video_writer = None
            camera.release()
            logger.info("[ODE] Camera released")

        return True
    

    def stop_detection(self):
        """Stop detection and clean up resources."""
        self._video_writer_failed = False
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


