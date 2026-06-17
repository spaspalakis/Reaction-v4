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
from functions.kafka_payload import build_detection_entry, build_frame_entry
from functions.logger import setup_logger   
from functions.logger import is_quiet_terminal
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
        self._last_frame_outcome = {}

    def _set_frame_outcome(self, frame_id, raw_count=0, final_count=0, reason="empty", classes=None):
        """Record process_frame result; logged once per cycle in run()."""
        self._last_frame_outcome = {
            "frame_id": frame_id,
            "raw_count": raw_count,
            "final_count": final_count,
            "reason": reason,
            "classes": list(classes or []),
        }

    def _format_class_list(self, labels):
        """Unique class names for terminal output, e.g. human/car."""
        names = []
        seen = set()
        for lbl in labels:
            if isinstance(lbl, (int, np.integer)):
                name = self._class_label(lbl)
            else:
                name = str(lbl)
            if name not in seen:
                seen.add(name)
                names.append(name)
        return "/".join(names) if names else "?"

    def _should_info_log(self, detections_count: int) -> bool:
        interval = self.config.get("info")
        if interval is None:
            return False
        return detections_count % max(1, int(interval)) == 0

    def _log_detection(self, frame_id, detections_count=0, kafka_msg_id=None):
        """Green terminal line when at least one object was detected."""
        outcome = self._last_frame_outcome or {}
        final = outcome.get("final_count", 0)
        reason = outcome.get("reason", "empty")
        classes = outcome.get("classes", [])

        if reason not in ("raw_mode", "detected") or final <= 0:
            if self.config.get("verbose") and reason in ("tracker_pending", "overlap_filtered"):
                raw = outcome.get("raw_count", 0)
                if reason == "tracker_pending":
                    msg = f"frame {frame_id} | infer: {raw} boxes, tracker pending"
                else:
                    msg = f"frame {frame_id} | infer: {raw} boxes, overlap filtered"
                if not is_quiet_terminal():
                    dt.print_green(msg)
            return

        if not self._should_info_log(detections_count):
            return

        classes_str = self._format_class_list(classes)
        if is_quiet_terminal():
            return
        if kafka_msg_id is not None:
            dt.print_blank()
        dt.print_green(
            dt.format_infer_line(
                frame_id,
                final,
                classes_str,
                msg_id=kafka_msg_id,
                raw=(reason == "raw_mode"),
            )
        )

    def _log_run_summary(
        self,
        total_runtime_sec,
        total_frames_read,
        total_frames_inferred,
        detection_frames,
        kafka_batches,
        total_objects,
    ):
        if total_runtime_sec <= 0:
            return
        read_fps = total_frames_read / total_runtime_sec
        infer_fps = total_frames_inferred / total_runtime_sec
        runtime_min = total_runtime_sec / 60.0
        msg = (
            f"[ODE] run summary | runtime {runtime_min:.1f} min | "
            f"read {total_frames_read} frames ({read_fps:.1f}/s) | "
            f"infer {total_frames_inferred} frames ({infer_fps:.1f}/s) | "
            f"detection frames {detection_frames} | "
            f"kafka sent {kafka_batches} | objects {total_objects}"
        )
        if not is_quiet_terminal():
            print("\n")
            dt.print_green(msg)
        logger.info(msg)

    @staticmethod
    def _detection_rate_settings(config, vid_fps, is_stream_input):
        detection_fps = float(config.get("detection_fps") or 0)
        if detection_fps <= 0:
            return {
                "detection_fps": 0.0,
                "min_interval_sec": 0.0,
                "video_frame_stride": 1,
            }
        if is_stream_input:
            return {
                "detection_fps": detection_fps,
                "min_interval_sec": 1.0 / detection_fps,
                "video_frame_stride": 1,
            }
        return {
            "detection_fps": detection_fps,
            "min_interval_sec": 0.0,
            "video_frame_stride": max(1, int(round(vid_fps / detection_fps))),
        }

    @staticmethod
    def _wait_for_next_detection_slot(last_processed_at, min_interval_sec):
        if min_interval_sec <= 0:
            return last_processed_at
        now = time.time()
        wait = min_interval_sec - (now - last_processed_at)
        if wait > 0:
            time.sleep(wait)
        return time.time()

    @staticmethod
    def _read_capture_frame(camera, drain_buffer=False, max_drain=120):
        if drain_buffer:
            drained = 0
            while drained < max_drain and camera.grab():
                drained += 1
            return camera.retrieve()
        return camera.read()

    def _log_geo_debug(self, frame_id, telemetry_msg, msg_id, telemetry_age_sec=None):
        """Yellow terminal dump of drone telemetry + per-object bbox/obj_geolocation (Kafka send)."""
        if not self.config.get("debug_geo") or is_quiet_terminal():
            return

        if telemetry_msg is None:
            dt.print_yellow(f"[Geo] frame={frame_id} msg={msg_id} | NO TELEMETRY")
            return

        tel = telemetry_msg.get("telemetry", {}) or {}
        iso = telemetry_msg.get("iso_time", "?")
        age = f"{telemetry_age_sec:.2f}s" if telemetry_age_sec is not None else "?"
        drone_line = (
            f"[Geo] frame={frame_id} msg={msg_id} | tel_age={age} iso={iso} | "
            f"lat={tel.get('latitude')} lon={tel.get('longitude')} alt={tel.get('altitude')} | "
            f"heading={tel.get('heading')} gimbal={tel.get('gimbalAngle')}"
        )
        dt.print_yellow(drone_line)

        frame_data = self.detection_map.get(frame_id)
        if not frame_data:
            for fd in self.detection_map.values():
                frame_data = fd
                break
        if not frame_data:
            dt.print_yellow(f"[Geo]   (no detections in batch)")
            return

        geo = frame_data.get("GeoLocation", {})
        dt.print_yellow(
            f"[Geo]   frame GeoLocation: lat={geo.get('latitude')} "
            f"lon={geo.get('longitude')} alt={geo.get('altitude')}"
        )
        for det in frame_data.get("detections", []):
            bbox = det.get("bbox")
            obj_geo = det.get("obj_geolocation")
            dt.print_yellow(
                f"[Geo]   obj id={det.get('objectID')} class={det.get('class')} "
                f"bbox={bbox} obj_geo={obj_geo}"
            )

    def emit_detection_batch(self, frame_id, telemetry_msg, save_json_local: bool):
        """Build a single-frame detection payload for Kafka (see config sampling for send cadence)."""
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
        n_objects = sum(
            len(frame_data.get("detections", []))
            for frame_data in self.detection_map.values()
        )
        msg_id = message_obj.message["records"][0]["value"]["header"]["msgIdentifier"]

        telemetry_age = (
            self.kafka_handler.get_telemetry_age_sec()
            if self.config.get("debug_geo")
            else None
        )
        self._log_geo_debug(frame_id, telemetry_msg, msg_id, telemetry_age)

        message_json = message_obj.to_json()

        if save_json_local:
            with open(json_path, "w") as f:
                f.write(message_json)
            if self.config.get("verbose"):
                logger.info(f"[ODE] JSON saved: {json_path}")

        self.detection_map = {}
        return n_objects, msg_id, message_json



    def add_detection(self,img_size,telemetry_msg, ip, frame_id, object_id, object_class, confidence, bbox):
        """Store detection under the correct frameID."""
        
        telemetry = telemetry_msg.get("telemetry", {})
        payload_opts = self.config.get("kafka_payload", {})

        lat = telemetry.get("latitude") 
        lon = telemetry.get("longitude") 
        alt = telemetry.get("altitude") 

        if frame_id not in self.detection_map:
            self.detection_map[frame_id] = build_frame_entry(
                frame_id,
                payload_opts=payload_opts,
                frame_image=self._current_frame_image,
                jpeg_quality=self.config.get("image_jpeg_quality", 80),
                latitude=lat,
                longitude=lon,
                altitude=alt,
            )
        fov = (68, 40)

        heading = telemetry.get("heading") 
        pitch =  telemetry.get("gimbalAngle") 
        pitch+=90

        drone_info = (lat, lon, alt, heading, pitch)
        
        # Calculate center pixel of bbox
        x1, y1, x2, y2 = bbox    
        center_pixel = (int((x1 + x2) / 2), int((y1 + y2) / 2))
        obj_gps = pixel_to_gps(center_pixel, img_size, fov, drone_info)

        self.detection_map[frame_id]["detections"].append(
            build_detection_entry(
                payload_opts=payload_opts,
                object_id=object_id,
                object_class=object_class,
                confidence=confidence,
                bbox=bbox,
                obj_gps=obj_gps,
            )
        )

    def finalize_detections(self, message_obj):
        """Convert stored detections into the correct JSON structure."""
        for frame_data in self.detection_map.values():
            message_obj.message["records"][0]["value"]["header"]["body"]["detection_list"].append(frame_data)

    def _class_label(self, class_idx):
        idx = int(class_idx)
        if 0 <= idx < len(self.class_names):
            return self.class_names[idx]
        return str(idx)

    def _process_raw_detections(self, original_image, boxes, classes, scores, frame_id, ip, img_size, telemetry_msg):
        """Send model boxes immediately without waiting for confirmed DeepSORT tracks."""
        labels = [self._class_label(c) for c in classes]
        track_ids = [0] * len(boxes)
        self._set_frame_outcome(
            frame_id,
            raw_count=len(boxes),
            final_count=len(boxes),
            reason="raw_mode",
            classes=labels,
        )

        annot_img = dspl_bboxes_cv3.display_bboxes(
            original_image,
            bboxes=boxes,
            labels=labels,
            title='fixed',
            scores=scores,
            tracks=track_ids,
            font_size=FONT_SIZE,
        )
        self._current_frame_image = annot_img

        for i in range(len(boxes)):
            if self.config.get("verbose", False):
                print(
                    "[Verbose] "
                    f"frame={frame_id} track_id=0 "
                    f"class={labels[i]} confidence={float(scores[i]):.3f} (raw)"
                )
            self.add_detection(
                img_size,
                telemetry_msg,
                ip=ip,
                frame_id=frame_id,
                object_id=0,
                object_class=labels[i],
                confidence=float(scores[i]),
                bbox=boxes[i].tolist(),
            )

        return annot_img

    def process_frame(self, image, infer, frame_id, ip,img_size,telemetry_msg):
        """Process a single frame and return annotated image and detected tracks."""
        self._last_frame_outcome = {}

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

        scores = np.atleast_1d(scores)
        classes = np.atleast_1d(classes)
        raw_count = len(scores)
        if raw_count == 0:
            self._set_frame_outcome(frame_id, reason="empty")
            return None

        boxes = np.reshape(boxes, (-1, 4))

        if self.config.get("raw_detections"):
            return self._process_raw_detections(
                original_image, boxes, classes, scores,
                frame_id, ip, img_size, telemetry_msg,
            )

        if raw_count > 0:
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

            if len(track_ids) == 0:
                self._set_frame_outcome(
                    frame_id, raw_count=raw_count, reason="tracker_pending"
                )
                return None

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

                if len(filtered_boxes) == 0:
                    self._set_frame_outcome(
                        frame_id, raw_count=raw_count, reason="overlap_filtered"
                    )
                    return None

                self._set_frame_outcome(
                    frame_id,
                    raw_count=raw_count,
                    final_count=len(filtered_boxes),
                    reason="detected",
                    classes=filtered_labels,
                )

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
        is_stream_input = input_mode in ("stream", "usb") or not is_likely_file

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

        rate_settings = self._detection_rate_settings(config, vid_fps, is_stream_input)
        detection_fps = rate_settings["detection_fps"]
        min_interval_sec = rate_settings["min_interval_sec"]
        video_frame_stride = rate_settings["video_frame_stride"]
        sampling = max(1, int(config.get("sampling", 1)))
        kafka_delay_sec = float(config.get("kafka_delay_sec", 0))
        rate_log_interval = float(config.get("info_detection_rate_every", 10.0))

        send_raw = bool(config.get("raw_detections"))
        tracker_mode = "raw" if send_raw else "deepsort"
        rate_mode = (
            f"{detection_fps:.1f} fps"
            if detection_fps > 0
            else "unlimited"
        )
        info_mode = (
            f"info={int(config['info'])}"
            if config.get("info") is not None
            else "info=off"
        )
        if not is_quiet_terminal():
            dt.print_green(
                f"[ODE] started | stream={vid_fps:.0f}fps | "
                f"infer_limit={rate_mode} | tracker={tracker_mode} | "
                f"sampling={sampling} | {info_mode} | score>={config.get('score_thres')}"
            )
        else:
            logger.info(
                f"[ODE] started | stream={vid_fps:.0f}fps | "
                f"infer_limit={rate_mode} | tracker={tracker_mode} | "
                f"sampling={sampling} | {info_mode} | score>={config.get('score_thres')}"
            )

        last_processed_at = 0.0
        frames_read = 0
        frames_processed = 0
        total_run_frames_read = 0
        total_run_frames_inferred = 0
        rate_log_start = time.time()

        try:
            while camera.isOpened() and self.kafka_handler.running:
                in_zone = self.kafka_handler.is_drone_in_polygon(polygon_flag)
                if was_in_detection_zone and not in_zone:
                    self.kafka_handler.send_end_session_signal()
                was_in_detection_zone = in_zone

                drain_buffer = bool(in_zone and detection_fps > 0 and is_stream_input)
                if drain_buffer:
                    last_processed_at = self._wait_for_next_detection_slot(
                        last_processed_at, min_interval_sec
                    )

                stream_diag.read_attempt()
                ret, image = self._read_capture_frame(camera, drain_buffer=drain_buffer)
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

                frames_read += 1
                total_run_frames_read += 1
                if rate_log_interval > 0 and config.get("info") is not None:
                    now = time.time()
                    elapsed = now - rate_log_start
                    if elapsed >= rate_log_interval:
                        kafka_queued, kafka_ack, kafka_failed = (
                            self.kafka_handler.consume_kafka_rate_stats()
                        )
                        msg = (
                            f"[ODE] rate ({frames_read / elapsed:.1f} read/s) | "
                            f"{frames_processed / elapsed:.1f} infer/s | "
                            f"{kafka_queued / elapsed:.1f} kafka/s ({kafka_queued} queued) | "
                            f"{kafka_ack / elapsed:.1f} ack/s ({kafka_ack} ack)"
                        )
                        if kafka_failed:
                            msg += f" | {kafka_failed} failed"
                        if not is_quiet_terminal():
                            print("\n")
                            dt.print_cyan(msg)
                        else:
                            logger.info(msg)
                        frames_read = 0
                        frames_processed = 0
                        rate_log_start = now

                if not in_zone:
                    fr_count += 1
                    continue

                if detection_fps > 0 and not is_stream_input and (fr_count % video_frame_stride != 0):
                    fr_count += 1
                    continue

                frames_processed += 1
                total_run_frames_inferred += 1
                telemetry_msg = self.kafka_handler.get_latest_telemetry_message()

                current_frame_id = fr_count
                output_image = self.process_frame(image, infer, current_frame_id, self.ip, img_size, telemetry_msg)

                if output_image is None:
                    no_detection_streak += 1
                    fr_count += 1
                    continue

                fr_count += 1
                detections_count += 1
                total_detected_objects += len(
                    self.detection_map.get(current_frame_id, {}).get("detections", [])
                )
                no_detection_streak = 0
                info_log = self._should_info_log(detections_count)

                if detections_count % sampling == 0:
                    n_objects, msg_id, message_json = self.emit_detection_batch(
                        current_frame_id, telemetry_msg, save_json
                    )
                    batches_sent += 1
                    self._log_detection(
                        current_frame_id,
                        detections_count=detections_count,
                        kafka_msg_id=msg_id,
                    )
                    self.kafka_handler.add_detection(message_json, terminal_log=info_log)
                    if kafka_delay_sec > 0:
                        time.sleep(kafka_delay_sec)
                else:
                    self._log_detection(current_frame_id, detections_count=detections_count)
                    self.detection_map = {}

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
            self._log_run_summary(
                total_runtime_sec,
                total_run_frames_read,
                total_run_frames_inferred,
                detections_count,
                batches_sent,
                total_detected_objects,
            )
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


