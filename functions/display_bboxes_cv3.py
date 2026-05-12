import os
import cv2 as cv
import numpy as np

THICKNESS = 2  # Line thickness for bounding boxes

# BGR. Keys must be lowercase (labels are matched with .strip().lower()).
_COLOR_MAP = {
    "boat": (197, 176, 213),
    "car": (255, 243, 126),
    "human": (59, 21, 185),
    # reaction-sea.names (land uses boat; sea uses sailboat/skiff/cruiseboat)
    "sailboat": (80, 180, 255),
    "skiff": (60, 220, 120),
    "cruiseboat": (180, 120, 255),
}

# Distinct fallback colors (avoid near-black so outlines stay visible).
_FALLBACK_BGR = [
    (0, 255, 255),
    (255, 128, 0),
    (255, 0, 255),
    (0, 165, 255),
    (147, 20, 255),
    (255, 191, 0),
]


def _color_for_class_label(label):
    key = (label or "").strip().lower()
    if key in _COLOR_MAP:
        return _COLOR_MAP[key]
    h = sum(ord(c) for c in key) if key else 0
    return _FALLBACK_BGR[h % len(_FALLBACK_BGR)]


def _text_bgr_for_background(bg_bgr):
    """Black or white text for readability on the label background."""
    b, g, r = bg_bgr
    luminance = 0.114 * b + 0.587 * g + 0.299 * r
    return (0, 0, 0) if luminance > 165 else (255, 255, 255)


def display_bboxes(image_source, bboxes, labels=None, scores=None, tracks=None, title=None, font_size=None):
    """
    Display the bboxes over the image using an array as parameter plus providing a label for the objects.
    :param font_size: Font size for text.
    :param title: Title of the plot (used only if plot is True).
    :param labels: List of labels for each bounding box.
    :param bboxes: Array of bounding boxes with shape [N, 4].
    :param image_source: Source of the image, can be a path, numpy array or cv2 image.
    :param plot: If True, display the image with matplotlib.
    :return: Image with bounding boxes drawn.
    """

    if labels is None:
        labels = []
        display_labels = False
        print('\nlabels is None')
    else:
        display_labels = True
        if not isinstance(labels, list):
            labels = [labels]

    if scores is None:
        scores = []
        display_scores = False
        print('\nscores is None')

    else:
        display_scores = True

    # if tracks is None:
    #     tracks = []
    #     display_tracks = False
    #     print('\ntracks is None')
        
    # else:
    #     display_tracks = True

    if not tracks :
        tracks = []
        display_tracks = False
        # print('[display_cv3] tracks is None')
        
    else:
        display_tracks = True

    if isinstance(image_source, np.ndarray):
        image = image_source
    elif os.path.exists(image_source):
        image = cv.imread(image_source)
    else:
        raise ValueError("Invalid image source")

       # Convert bboxes to a numpy array if it isn't one already
    # bboxes = np.array(bboxes)

    for i in range(bboxes.shape[0]):
    # for i in range(len(bboxes)):

        label = labels[i]
        color = _color_for_class_label(label)
        
        if display_labels:
            if display_scores:

                conf_perce = scores[i] * 100  # Convert to percentage
                if not display_tracks:
                    # display_str_list = ['{}: {:1.3f}'.format(labels[i], scores[i])]
                    display_str_list = ['{}: {:1.1f}'.format(labels[i], conf_perce)]
                    # print('[display_cv3] Not display tracks')
                else:
                    # display_str_list = ['{}: {:1.3f} tr-ID: {}'.format(labels[i], scores[i], tracks[i])]
                    # track_label = "New" if tracks[i] in new_tracks else f"ID:{tracks[i]}"
                    display_str_list = ['{}: {:1.1f} ID:{}'.format(labels[i], conf_perce, tracks[i])]


            else:
                if display_tracks:
                    display_str_list = ['{}: ID:{}'.format(labels[i], tracks[i])]
                else:
                    display_str_list = [labels[i]]
        else:
            display_str_list = ['']

        draw_bounding_box_on_image(image, bboxes[i, 1], bboxes[i, 0], bboxes[i, 3], bboxes[i, 2],
                                   color=color, thickness=THICKNESS, display_str_list=display_str_list,
                                   use_normalized_coordinates=False, font_size=font_size)

    # if plot:
    #     if title is None:
    #         title = 'Bounding Boxes'
    #     plt.figure(title, figsize=(16, 12), dpi=80)
    #     plt.imshow(cv.cvtColor(image, cv.COLOR_BGR2RGB))
    #     plt.waitforbuttonpress()
    #     plt.close()

    return image

def draw_bounding_box_on_image(image, ymin, xmin, ymax, xmax, color=(0, 255, 0), thickness=4,
                               display_str_list=(), use_normalized_coordinates=True, font_size=None):
    """Adds a bounding box to an image using OpenCV.

    Args:
        image: a numpy array representing the image.
        ymin: ymin of bounding box.
        xmin: xmin of bounding box.
        ymax: ymax of bounding box.
        xmax: xmax of bounding box.
        color: color to draw bounding box (BGR format).
        thickness: line thickness.
        display_str_list: list of strings to display in box.
        use_normalized_coordinates: If True, treat coordinates as relative to the image.
        font_size: Font size for text.
    """
        
    im_height, im_width, _ = image.shape
    if use_normalized_coordinates:
        (left, right, top, bottom) = (int(xmin * im_width), int(xmax * im_width),
                                      int(ymin * im_height), int(ymax * im_height))
    else:
        (left, right, top, bottom) = (int(xmin), int(xmax), int(ymin), int(ymax))
    
    # Draw bounding box
    cv.rectangle(image, (left, top), (right, bottom), color, thickness)

    if display_str_list:
        for display_str in display_str_list[::-1]:
            text_size, baseline = cv.getTextSize(display_str, cv.FONT_HERSHEY_SIMPLEX, font_size / 24, 1)
            text_width, text_height = text_size
            margin = int(0.05 * text_height)
            
            text_x = max(left, 0)
            text_y = max(top - text_height - 2 * margin, 0)
            
            # Adjust position if text is outside the top boundary
            if text_y < 0:
                text_y = top + text_height + 2 * margin
            
            # Adjust position if text is outside the right boundary
            if text_x + text_width > im_width:
                text_x = im_width - text_width - margin

            # (text_x, text_y) = (left, top - text_height - 2 * margin)
            cv.rectangle(image, (text_x, text_y), (text_x + text_width, text_y + text_height + margin), color, cv.FILLED)
            text_color = _text_bgr_for_background(color)
            cv.putText(image, display_str, (text_x + margin, text_y + text_height - margin), cv.FONT_HERSHEY_SIMPLEX,
                       font_size / 24, text_color, 1)
