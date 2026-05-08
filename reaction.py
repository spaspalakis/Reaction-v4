"""
version 2:
This is an upgraded version from simple-Reactions.
It simulated a drone move: inside and outside a polygon
The detector is triggered automatically when the telemetry of the drone is in the polygon,
if the telemetry is outside the polygon or in a non-flight zone the detector just skipping the frames.

Args:
--save-json
--save-frames
--save-video
--drone-id 5 
--drone-name SIM_Alpha
--model land | sea   (sets model_path, labels_path, model_input_size; else use config.json)


-31/3 OOP logic introduced
-29/4 Sever logic introduced. Try to pass images over the internet and display them into the dashboard
-12/5 Changed the message stucture. Each JSON has one detection
-20/5 Change the triggering concept : 1. Create consumer for Path Planning 
                                     2. Trigger the OD when the telemetry enters the polygon defined by the PP
                                     3. Updated the format of the message 
-2/6 Create random drone move based the Path Planning polygon
-11/6 1. get_external_ip method is now an stand alone fucntion
     2. KafkaHandler introduced for thread-safe Kafka operations

--24/6 Change message structure: add obj_geolocation (Final version)
--25/6 Drone is triggered correctly when enters the polygon
--26/6 Filter UAV telemetry use --drone-id or --drone-name



version 3:
27/6 Change how handle the incomming messages. Deleted the dictionary current_metadata and added geters in kafka_handler

version 4:
-3/4 Change the imgURL format. Now it is a base64 string. 
"""

import os
import sys

# Must run before TensorFlow and before importing modules that print at import time.
if "--quiet" in sys.argv:
    os.environ["REACTION_QUIET"] = "1"
    os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

import numpy as np
import cv2 as cv
import tensorflow as tf
import time
from datetime import timedelta
import json
from shapely.geometry import Point
from functions.logger import setup_logger
from functions.logger import is_quiet_terminal
from functions.logger import format_run_timestamp, attach_run_log_file

os.environ['CUDA_VISIBLE_DEVICES'] = "0"

### Tensorflow ####
from tensorflow.python.saved_model import tag_constants

### import functions ###
from functions import arguments
from functions import display_tools as dt
from functions import user_input
from functions import check_folder_paths
from functions import ODE_v3
from functions.kafka_handler import KafkaHandler
logger = setup_logger()

def main():

    cv.ocl.setUseOpenCL(False)

    # Load arguments
    args = arguments.get_arguments()
    start1 = time.time()
    
    # Load configuration
    with open("functions/config.json", "r") as config_file:
        config = json.load(config_file)
    config["verbose"] = bool(args.verbose)

    if args.model:
        preset = arguments.MODEL_PRESETS[args.model]
        config["model_path"] = preset["model_path"]
        config["labels_path"] = preset["labels_path"]
        config["model_input_size"] = preset["model_input_size"]

    # Determine input source based on CLI arguments and config
    if args.usb_cam is not None:
        input_mode = "usb"
        source_path = args.usb_cam if isinstance(args.usb_cam, str) else config["usb_cam_device"]
        input_name = "usb-cam"
    elif args.rtsp:
        input_mode = "rtsp"
        source_path = config["rtsp_link"]
        input_name = "rtsp"
    else:
        # Default to local video file (or when --video is provided)
        input_mode = "video"
        source_path = config["video_input_path"]
        input_name = os.path.splitext(os.path.basename(source_path))[0]

    from datetime import datetime as _dt

    now = _dt.now()
    run_ts = format_run_timestamp(now)
    run_root = os.path.join("runs", run_ts)
    attach_run_log_file(run_root, run_ts)
    config["stream_test_enabled"] = bool(args.test_stream)
    if args.test_stream:
        config["stream_test_file"] = os.path.join(run_root, "test-stream")
    if args.save_frames:
        config["frames_folder"] = os.path.join(run_root, "frames")
    if args.save_json:
        config["json_folder"] = os.path.join(run_root, "json")
    if args.save_video:
        config["video_output_folder"] = os.path.join(run_root, "video")
        config["video_output_filename"] = f"{input_name}.mp4"

    if args.model:
        logger.info(
            f"[Main] Model preset '{args.model}': path={config['model_path']}, "
            f"labels={config['labels_path']}, input={config['model_input_size']}"
        )

    check_folder_paths.folder_paths(
        config["frames_folder"],
        config["json_folder"],
        config["video_input_path"],
        config["video_output_folder"],
        save_frames=args.save_frames,
        save_json=args.save_json,
        save_video=args.save_video,
    )

    # Check user input: USB CAMERA, LOCAL VIDEO or RTSP
    camera, img_size = user_input.check_user_input(input_mode, source_path)
    

    # Load model
    logger.info("Model is loading...")  
    time1_loadModel = time.time()
    saved_model_loaded = tf.saved_model.load(config["model_path"], tags=[tag_constants.SERVING])
    infer = saved_model_loaded
    time2_loadModel = time.time()

    time_loadModel = divmod(time2_loadModel - time1_loadModel, 60)
    if not is_quiet_terminal():
        dt.print_blue('\nModel load time:')
        print('Minutes: ', time_loadModel[0], '\nSeconds: ', time_loadModel[1], '\n')

    # Select Kafka broker (VPN or public)
    selected_broker = config.get("broker_vpn") if getattr(args, "vpn_kafka", False) else config["broker"]
    logger.info(f"[Main] Using Kafka broker: {selected_broker}")

    # Initialize Kafka Handler
    kafka_handler = KafkaHandler(
        broker=selected_broker,
        producer_topic=config['producer_topic'],
        path_planning_topic=config['path_planning_topic'],
        UAV_Telemetry_topic=config['UAV_Telemetry_topic'],
        selected_drone_id=args.drone_id,
        selected_drone_name=args.drone_name
    )
    kafka_handler.start()
    
    # Create detector instance
    detector = ODE_v3.ObjectDetector(config, kafka_handler)
    logger.info(f'[Main] Listen drone {args.drone_name} with ID: {args.drone_id}')

    if args.polygon:
        logger.info("[Main] POLYGON mode ON")
        logger.info("[Main] Waiting for path planning message...")
        while not kafka_handler.get_latest_pp_message():
            time.sleep(1)
        logger.info("[Main] Path planning message received!")
        
        logger.info("[Main] Waiting for telemetry message...")
        while not kafka_handler.get_latest_telemetry_message():
            time.sleep(1)
        logger.info("[Main] Telemetry message received!")

    else:
        logger.info("[Main] POLYGON mode OFF")

        # Check if a path planning message arrives before telemetry
        pp_msg = kafka_handler.get_latest_pp_message()
        if pp_msg:
            logger.info("[Main] Path planning message received (no polygon mode)")

        logger.info("[Main] Waiting for telemetry message...")
        while not kafka_handler.get_latest_telemetry_message():
            pp_msg = kafka_handler.get_latest_pp_message()
            if pp_msg:
                logger.info("[Main] Path planning message received while waiting for telemetry")
            time.sleep(1)
        logger.info("[Main] Telemetry message received!")


    try:
        while True:

            # Check if drone is in valid position
            if kafka_handler.is_drone_in_polygon(args.polygon):
                logger.info("[Main] Drone is INSIDE the polygon. Starting detection...")
                if not is_quiet_terminal():
                    dt.print_blue('\n[Main] Check inside polygon from MAIN')

                # Start detection loop
                result = detector.run(
                    img_size,
                    config,
                    camera,
                    infer,
                    args.save_frames,
                    args.save_json,
                    args.polygon,
                    save_video=args.save_video,
                    source_path=source_path,
                    input_mode=input_mode,
                )
                
                if result is False:
                    if not is_quiet_terminal():
                        print("[Main] Video ended or camera released. Exiting main loop.")
                    break
            # else:
            #     logger.info('[Main] Drone outside polygon')            

            time.sleep(0.1)  # Prevent busy waiting
    except KeyboardInterrupt:
         logger.info("[Main] Interrupted by user. Exiting...")
     
    finally:
         detector.stop_detection()
         kafka_handler.stop()
         
    # print('\n[Main] - Process stopped')         
    logger.info("[Main] - Process stopped")         

    end = time.time()
    # print('Total elapsed time parsing main loop was {:2.3f} sec.'.format(end - start1))
    logger.info('Total elapsed time parsing main loop was {:2.3f} sec.'.format(end - start1))

if __name__ == '__main__':
    main()
