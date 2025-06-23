import cv2 as cv
import os
# import device

from functions import display_tools as dt


def check_user_input(use_cam,rtsp_link):

    # device_list = device.getDeviceList()
    # dt.print_red(f"\ndevice_list: {device_list}")
    
    print(f'\nCheck user input:')
    #### Check user input read VIDEO or CAMERA ####
    if use_cam:
        dt.print_green('Using HDMI input ..')
        # camera = cv.VideoCapture("v4l2src device=/dev/video0 ! video/x-raw, format=YUY2 ! videoconvert ! appsink", cv.CAP_GSTREAMER)
        camera = cv.VideoCapture("/dev/video0") 
        # camera = cv.VideoCapture(2) # OBS stream
        # camera = cv.VideoCapture("rtmp://127.0.0.1/live") # RTMP stream


        dt.print_green(f'Camera is open: {camera.isOpened()}')
        video_name = 'hdmi'

        fps = camera.get(cv.CAP_PROP_FPS)
        frame_height = int(camera.get(cv.CAP_PROP_FRAME_HEIGHT))
        frame_width = int(camera.get(cv.CAP_PROP_FRAME_WIDTH))  
        img_size = (frame_width, frame_height)
        dt.print_green(f'\nFPS: {fps}\n(H,W): {frame_height,frame_width}')
        
        if not camera.isOpened():
            dt.print_red('Camera not opened. Retrying...')
            for n in range(5):
                # camera = cv.VideoCapture("v4l2src device=/dev/video0 ! video/x-raw, format=YUY2, width=1280, height=720, pixel-aspect-ratio=1/1, framerate=30/1 ! videoconvert ! appsink",cv.CAP_GSTREAMER)
                camera = cv.VideoCapture("/dev/video0")
                if camera.isOpened():
                    break
            else:
                raise ValueError('Could not get HDMI input from Jetson after 5 attewpts. Aborting...')

    else:
        dt.print_green('Reading VIDEO from folder...')           
        camera = cv.VideoCapture(rtsp_link)
        frame_width = int(camera.get(cv.CAP_PROP_FRAME_WIDTH))
        frame_height = int(camera.get(cv.CAP_PROP_FRAME_HEIGHT))
        img_size = (frame_width, frame_height)

        if not camera.isOpened():
            print('Error: Could not open video file.')
            exit()
        else:
            print(f'Camera is open: {camera.isOpened()}')
        
        fps = camera.get(cv.CAP_PROP_FPS) # Get the frame rate of the video
        print(f'Video file input: ./{rtsp_link} | FPS: {int(fps)} ')
        dt.print_green(f'Video exists: {os.path.exists(rtsp_link)}\n')
        
        frame_count = int(camera.get(cv.CAP_PROP_FRAME_COUNT))  # Get the total number of frames in the vide

        video_name = os.path.splitext(os.path.basename(rtsp_link))[0]
 


    return camera,img_size