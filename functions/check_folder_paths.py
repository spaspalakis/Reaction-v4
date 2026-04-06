import os


def folder_paths(frames_folder, json_folder, video_path, video_output,
                  save_frames=False, save_json=False, save_video=False):

    if save_frames:
        os.makedirs(frames_folder, exist_ok=True)

    if save_json:
        os.makedirs(json_folder, exist_ok=True)

    if save_video:
        os.makedirs(video_output, exist_ok=True)


