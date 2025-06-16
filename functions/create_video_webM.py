import os
import cv2
import numpy as np
import skvideo.io
from tqdm import tqdm

from functions import display_tools as dt

def get_last_saved_number():
        # Check if the file containing the last saved number exists
        if os.path.exists("video_counter.txt"):
            with open("video_counter.txt", "r") as file:
                last_saved_number = int(file.read())
        else:
            last_saved_number = 0
        return last_saved_number


def save_last_number(number):
        with open("video_counter.txt", "w") as file:
            file.write(str(number))



def video_writer(video_output_folder,frame_width,frame_height):

    """
    Initalize the video writer. Creats videos in ascending order
    """


    # Codec settings
    codec = 'VP80'  # Codec for webM format

    # Create a video writer object
    fourcc = cv2.VideoWriter_fourcc(*codec)

    # Reads from video_counter.txt and get the last saved number
    video_counter = get_last_saved_number() 

    # Create the video output path
    output_video_path = os.path.join(video_output_folder,'video_{}.{}'.format(video_counter, 'webm'))
    # print('\nWriting file: {}...'.format(output_video_path))
    video_name = output_video_path.split("/")[-1]  # video_name: video_0.mp4

    # Create the video writer
    video_writer = cv2.VideoWriter(output_video_path, fourcc, 20, (frame_width, frame_height))
    dt.print_green(f"\n[Video Writer] Creating video: {video_name}")

    # Update the last saved number
    save_last_number(video_counter + 1)


    return video_writer
