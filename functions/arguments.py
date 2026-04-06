import os
import argparse
import platform

from functions import display_tools as dt


def _quiet_env():
    return os.environ.get("REACTION_QUIET", "").lower() in ("1", "true", "yes")


release_info = platform.release()
if not _quiet_env():
    print('\nrelease_info: {}'.format(release_info))

if 'tegra' in platform.release():
    if not _quiet_env():
        dt.print_green('\nInside Jetson platform')
    VIDEO_FOLDER =  '/home/certh/treeads_v4/videos/' # '~/videos/'
    # USE_HDMI = True
else:
    if not _quiet_env():
        dt.print_green('Inside typical Ubuntu workstation.')
    VIDEO_FOLDER = 'video_input/'
    # USE_HDMI = False  # True  #

DETECTION_RATE = 5  # 2  #

# SavedModel dirs must match imgsz in each model's metadata.yaml (Ultralytics export).
MODEL_PRESETS = {
    "land": {
        "model_path": "models/model-land",
        "labels_path": "models/reaction-land.names",
        "model_input_size": 736,
    },
    "sea": {
        "model_path": "models/model-sea",
        "labels_path": "models/reaction-sea.names",
        "model_input_size": 640,
    },
}


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
    
    parser.add_argument("--usb-cam", action="store_true",
                        help="Use USB camera input (e.g. /dev/video0)")
    
    parser.add_argument("--video", action="store_true",
                        help="Use local video file defined in config.json")
    
    parser.add_argument("--rtsp", action="store_true",
                        help="Use RTSP stream link defined in config.json")
    
    parser.add_argument("--vpn-kafka", action="store_true",
                        help="Use Kafka broker over VPN (broker_vpn in config.json)")
    
    parser.add_argument("--quiet", action="store_true",
                        help="Terminal: only prints from Kafka send path; full logs still written to logs/")
    
    parser.add_argument("--plot-frames", action="store_true",
                        help="Plot every detected frame. Default value is False")
    
    parser.add_argument("--save-frames", action="store_true",
                        help="Write image into folder")
    
    parser.add_argument("--save-video", action="store_true",
                        help="Write annotated detection video (MP4) under video_output_folder")

    parser.add_argument("--save-json", action="store_true",
                        help="Write detection JSON files under json_folder (Kafka is published whenever a batch is emitted, even without this flag)")
    
    parser.add_argument("--drone-name", type=str, default=None,
                    help="The drone name to listen for in the telemetry topic. If not set, all names are accepted.")
    
    parser.add_argument("--drone-id", type=int, default=None,
                        help="The drone ID to listen for in the telemetry topic. Default: None")
    
    parser.add_argument("--polygon", action="store_true",
                        help="Parameter for polygon. Default: No polygon needed")

    parser.add_argument(
        "--model",
        type=str,
        choices=tuple(MODEL_PRESETS.keys()),
        default=None,
        help="Detection preset: land (736², models/model-land) or sea (640², models/model-sea). "
             "Sets model_path, labels_path, model_input_size. "
             "If omitted, those three come from config.json.",
    )

    return parser.parse_args()



