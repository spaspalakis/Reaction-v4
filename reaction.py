"""
version 2:

-31/3 OOP logic introduced
-29/4 Sever logic introduced. Try to pass images over the internet and display them into the dashboard
-12/5 Changed the message stucture. Each JSON has one detection
-20/5 Change the triggering concept : 1. Create consumer for Path Planning 
                                     2. Trigger the OD when the telemetry enters the polygon defined by the PP
                                     3. Updated the format of the message 
-2/6 Create random drone move based the Path Planning polygon
-11/6 1. get_external_ip method is now an stand alone fucntion
     2. KafkaHandler introduced for thread-safe Kafka operations
"""

import os
import numpy as np
import cv2 as cv
import tensorflow as tf
import time
from datetime import timedelta
import json
import sys
from shapely.geometry import Point
from functions.logger import setup_logger

# Configure GPU memory
gpus = tf.config.experimental.list_physical_devices('GPU')
for gpu in gpus:
    tf.config.set_logical_device_configuration(gpus[0], [tf.config.LogicalDeviceConfiguration(memory_limit=4096)])

os.environ['CUDA_VISIBLE_DEVICES'] = "0"

### Tensorflow ####
from tensorflow.python.saved_model import tag_constants

### import functions ###
from functions import arguments
from functions import display_tools as dt
from functions import user_input
from functions import check_folder_paths
from functions import ODE_v6
from functions.kafka_handler import KafkaHandler

def main():
    """
    The main function to perform object detection without threading.

    1. Initialize the arguments
    2. Check folder paths if they exist
    3. Check user input: Read from camera or video
    4. Initialize UDP communication
    5. Load model
    6. Start object detection based on UAV status
    """
    cv.ocl.setUseOpenCL(False)

    # Load arguments
    args = arguments.get_arguments()
    start1 = time.time()
    
    # Load configuration
    with open("functions/config.json", "r") as config_file:
        config = json.load(config_file)
    
    # Check folder paths
    check_folder_paths.folder_paths(
        config["frames_folder"],
        config["json_folder"],
        config["video_input_path"],
        config["video_output_folder"],
        args.init_vc
    )

    # Check user input: Read VIDEO or CAMERA
    camera, video_name, frame_width, frame_height = user_input.check_user_input(args.use_cam, config["video_input_path"])

    # Update config with frame dimensions
    config["frame_width"] = frame_width
    config["frame_height"] = frame_height
    
    # Save updated config
    with open("functions/config.json", "w") as config_file:
        json.dump(config, config_file, indent=4)

    # Load model
    dt.print_green("\n-----\nModel is loading...")  
    time1_loadModel = time.time()
    saved_model_loaded = tf.saved_model.load(config["model_path"], tags=[tag_constants.SERVING])
    infer = saved_model_loaded
    time2_loadModel = time.time()

    time_loadModel = divmod(time2_loadModel - time1_loadModel, 60)
    dt.print_blue('\nModel load time:')
    print('Minutes: ', time_loadModel[0], '\nSeconds: ', time_loadModel[1], '\n')

    # Initialize Kafka Handler
    kafka_handler = KafkaHandler(
        broker=config['broker'],
        producer_topic=config['producer_topic'],
        command_control_topic=config['command_control_topic'],
        path_planning_topic=config['path_planning_topic']
    )
    kafka_handler.start()
    
    # Create detector instance
    detector = ODE_v6.ObjectDetector(config, kafka_handler)

    # Wait for initial path planning message
    logger = setup_logger()
    logger.info("[Main] Waiting for initial path planning message...")
    while not kafka_handler.get_latest_pp_message():
        time.sleep(1)
    logger.info("[Main] Received initial path planning message")

    try:
        while True:

            current_drone_data = kafka_handler.get_current_metadata()
            if current_drone_data["uav_status"] == "up":
                # Check if drone is in valid position
                if kafka_handler.is_drone_in_polygon():
                    # print("\n[Main] Drone is INSIDE the polygon. Starting detection...")
                    logger.info("[Main] Drone is INSIDE the polygon. Starting detection...")

                    detector.update_metadata(current_drone_data)
                    detector.run(config, camera, infer, args.create_video, args.save_frames)
                else:
                    # dt.print_yellow("\n[Main] Drone is OUTSIDE the polygon or in a NO-FLIGHT-ZONE.")
                    logger.warning("[Main] Drone is OUTSIDE the polygon or in a NO-FLIGHT-ZONE.")

                    detector.stop_detection()
                    
            elif current_drone_data["uav_status"] == "down":
                # print("\n[Main] UAV is down, waiting for status change...")
                logger.info("[Main] UAV is down, waiting for status change...")

                detector.stop_detection()
            elif current_drone_data["uav_status"] == "exit":
                # print("\n[Main] Exit signal received. Stopping...")
                logger.info("[Main] Exit signal received. Stopping...")
                
                break

            time.sleep(0.1)  # Prevent busy waiting

    except KeyboardInterrupt:
        # print("\n[Main] Interrupted by user. Exiting...")
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
