import os 
import sys

from functions import display_tools as dt


def folder_paths (frames_folder,json_folder,video_path,video_output):

    ### Check if snapshots folder path exists (frames that will be saved).
    print('\n---\n[Check folder paths] Initialize existance of path folders:')
    if not os.path.exists(frames_folder):
        os.makedirs(frames_folder)
        print("{} just created".format(frames_folder))
    
    ### Check if json folder path exists.
    if not os.path.exists(json_folder):
        os.makedirs(json_folder)
        print("{} just created".format(json_folder))

    if not os.path.exists(video_output):
        os.makedirs(video_output)
        print("{} just created".format(video_output))

    
    if os.path.exists(video_path) and os.path.exists(json_folder) and os.path.exists(video_output):
        dt.print_green('\nSnapshots will be written to : ./{}'.format(frames_folder))
        dt.print_green('JSON files will be written to : ./{}'.format(json_folder))
        dt.print_green('Video output will be written to : ./{}'.format(video_output))
    else:
        dt.print_red ("Some folder path does not exist! Check again!")
        sys.exit()


