import cv2 as cv
import os

def _quiet_env():
    return os.environ.get("REACTION_QUIET", "").lower() in ("1", "true", "yes")

from functions import display_tools as dt


def check_user_input(input_mode, source_path):
    q = _quiet_env()

    if not q:
        print(f'\nCheck user input:')
    if input_mode == "usb":
        if not q:
            dt.print_green('Using USB camera input ..')
        camera = cv.VideoCapture(source_path)

        if not q:
            dt.print_green(f'Camera is open: {camera.isOpened()}')
        video_name = 'hdmi'

        fps = camera.get(cv.CAP_PROP_FPS)
        frame_height = int(camera.get(cv.CAP_PROP_FRAME_HEIGHT))
        frame_width = int(camera.get(cv.CAP_PROP_FRAME_WIDTH))
        img_size = (frame_width, frame_height)
        if not q:
            dt.print_green(f'\nFPS: {fps}\n(H,W): {frame_height,frame_width}')

        if not camera.isOpened():
            dt.print_red('Camera not opened. Retrying...')
            for n in range(5):
                camera = cv.VideoCapture(source_path)
                if camera.isOpened():
                    break
            else:
                raise ValueError('Could not get USB input after 5 attempts. Aborting...')

    else:
        if input_mode == "rtsp":
            if not q:
                dt.print_green('Reading RTSP stream...')
        else:
            if not q:
                dt.print_green('Reading VIDEO from folder...')

        camera = cv.VideoCapture(source_path)
        frame_width = int(camera.get(cv.CAP_PROP_FRAME_WIDTH))
        frame_height = int(camera.get(cv.CAP_PROP_FRAME_HEIGHT))
        img_size = (frame_width, frame_height)

        if not camera.isOpened():
            print('Error: Could not open video/stream source.')
            exit()
        else:
            if not q:
                print(f'Camera/stream is open: {camera.isOpened()}')

        fps = camera.get(cv.CAP_PROP_FPS)
        if not q:
            print(f'Input source: ./{source_path} | FPS: {int(fps)} ')
        if input_mode == "video":
            if not q:
                dt.print_green(f'Video exists: {os.path.exists(source_path)}\n')

        frame_count = int(camera.get(cv.CAP_PROP_FRAME_COUNT))

        video_name = os.path.splitext(os.path.basename(source_path))[0] if input_mode == "video" else 'stream'

    return camera, img_size
