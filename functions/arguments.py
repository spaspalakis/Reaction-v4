import os
import argparse
import platform
from functions import display_tools as dt

release_info = platform.release()
print('\nrelease_info: {}'.format(release_info))

if 'tegra' in platform.release():
    dt.print_green('\nInside Jetson platform')
    VIDEO_FOLDER =  '/home/certh/treeads_v4/videos/' # '~/videos/'
    # USE_HDMI = True
else:
    dt.print_green('Inside typical Ubuntu workstation.')
    VIDEO_FOLDER = 'video_input/'
    # USE_HDMI = False  # True  #

DETECTION_RATE = 5  # 2  #
MISSION_ID = 1
BATCH_FRAMES = 5
USE_CPU = False  # True  #
MODEL_INPUT_SIZE = 736 #416
USE_HDMI = False



def get_arguments():
    """Parse all the arguments provided from the CLI.

    Returns:
      A list of parsed arguments.
    """
    parser = argparse.ArgumentParser(description="ODE Object detection")   

    
    parser.add_argument("--detection-rate", type=int, default=DETECTION_RATE,
                        help="The rate of detection, it perform 1 detection every detection-rate value. "
                             "Default: {}".format(DETECTION_RATE))

    parser.add_argument("--mission-id", type=int, default=MISSION_ID, 
                        help="The id of the mission assigned to the drone")
        

    ### ACTIONS
    
    parser.add_argument("--details", action="store_true",
                        help="Show details for the detected object")
    
    parser.add_argument("--use-cam", action="store_true",
                        help="Whether to use HDMI (the input from camera or not")
    
    parser.add_argument("--plot-frames", action="store_true",
                        help="Plot every detected frame. Default value is False")
    
    parser.add_argument("--save-frames", action="store_true",
                        help="Write image into folder")
        
    parser.add_argument("--save-json", action="store_true",
                        help="Save json into folder")
    
       
    return parser.parse_args()



