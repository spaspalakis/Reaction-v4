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

def get_arguments():
    """Parse all the arguments provided from the CLI.

    Returns:
      A list of parsed arguments.
    """
    parser = argparse.ArgumentParser(description="ODE Object detection")   

    
    parser.add_argument("--detection-rate", type=int, default=DETECTION_RATE,
                        help="The rate of detection, it perform 1 detection every detection-rate value. "
                             "Default: {}".format(DETECTION_RATE))
    
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
    
    parser.add_argument("--drone-name", type=str, default=None,
                    help="The drone name to listen for in the telemetry topic. If not set, all names are accepted.")
    
    parser.add_argument("--drone-id", type=int, default=1,
                        help="The drone ID to listen for in the telemetry topic. Default: 1")
       
    return parser.parse_args()



