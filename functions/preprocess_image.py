import numpy as np
import cv2 as cv

def preprocess_image(image, image_size):
    # image, RGB
    image_height, image_width = image.shape[:2]
    #  ################# resize image if needed
    if image_height > image_width:
        scale = image_size / image_height
        resized_height = image_size
        resized_width = int(image_width * scale)
    else:
        scale = image_size / image_width
        resized_height = int(image_height * scale)
        resized_width = image_size

    image = cv.resize(image, (resized_width, resized_height))  # just resizes with larger dim being 512

    image = image.astype(np.float32)
    image /= 255.
    # mean = [0.485, 0.456, 0.406]  # this shouldn't be the same for Pascal & Coco, right? This is for Pascal though
    # std = [0.229, 0.224, 0.225]
    # image -= mean
    # image /= std
    pad_h = image_size - resized_height
    pad_w = image_size - resized_width
    # image = np.pad(image, [(0, pad_h), (0, pad_w), (0, 0)], mode='constant')
    image = np.pad(image, [(pad_h - pad_h//2, pad_h//2), (pad_w - pad_w//2, pad_w//2), (0, 0)], mode='constant')

    # torch stuff or yolov5 stuff
    # image = image.transpose((2, 0, 1))[::-1]  # HWC to CHW, BGR to RGB
    # image = np.ascontiguousarray(image)  # contiguous
    image = np.expand_dims(image, axis=0)
    
    # image = image.transpose((0, 2, 3, 1))
    # im.permute(0, 2, 3, 1)

    return image  # , scale
