# Theoretically this gathers tools that are connected to visualization either in console or not
import os
import sys

import numpy as np
from PIL import Image, ImageDraw, ImageFont
import PIL
from matplotlib import pyplot as plt, pylab
import matplotlib
import cv2
# from mttkinter import mtTkinter
# import image_manipulation_tools as imt


def print2(args):
    """
    Just a helper function to print in the same line (without the newline that is) multiple values
    :param args: the typical argument to print
    :return: None
    """
    print(args, end="", flush=True)


def print_green(args):
    if isinstance(args, list):
        if len(args) == 2:
            print('{}\033[30;1;32m{}\033[00m'.format(args[0], args[1]))
        elif len(args) == 3:
            print('{}\033[30;1;32m{}\033[00m{}'.format(args[0], args[1], args[2]))
        elif len(args) == 1:
            print('\033[30;1;32m{}\033[00m'.format(args[0]))
    else:
        print('\033[30;1;32m{}\033[00m'.format(args))


# def print_blue(args):
#     if isinstance(args, list):
#         if len(args) == 2:
#             print('{}\033[30;1;34m{}\033[00m'.format(args[0], args[1]))
#         elif len(args) == 3:
#             print('{}\033[30;1;34m{}\033[00m{}'.format(args[0], args[1], args[2]))
#         elif len(args) == 1:
#             print('\033[30;1;34m{}\033[00m'.format(args[0]))
#     else:
#         print('\033[30;1;34m{}\033[00m'.format(args))

def print_blue(args):
    if isinstance(args, list):
        if len(args) == 2:
            print('{}\033[94m{}\033[00m'.format(args[0], args[1]))
        elif len(args) == 3:
            print('{}\033[94m{}\033[00m{}'.format(args[0], args[1], args[2]))
        elif len(args) == 1:
            print('\033[94m{}\033[00m'.format(args[0]))
    else:
        print('\033[94m{}\033[00m'.format(args))

def print_yellow(args):
    if isinstance(args, list):
        if len(args) == 2:
            print('{}\033[93m{}\033[00m'.format(args[0], args[1]))
        elif len(args) == 3:
            print('{}\033[93m{}\033[00m{}'.format(args[0], args[1], args[2]))
        elif len(args) == 1:
            print('\033[93m{}\033[00m'.format(args[0]))
    else:
        print('\033[93m{}\033[00m'.format(args))

def print_white(args):
    if isinstance(args, list):
        if len(args) == 2:
            print('{}\033[97m{}\033[00m'.format(args[0], args[1]))
        elif len(args) == 3:
            print('{}\033[97m{}\033[00m{}'.format(args[0], args[1], args[2]))
        elif len(args) == 1:
            print('\033[97m{}\033[00m'.format(args[0]))
    else:
        print('\033[97m{}\033[00m'.format(args))

def print_magenta(args):
    if isinstance(args, list):
        if len(args) == 2:
            print('{}\033[95m{}\033[00m'.format(args[0], args[1]))
        elif len(args) == 3:
            print('{}\033[95m{}\033[00m{}'.format(args[0], args[1], args[2]))
        elif len(args) == 1:
            print('\033[95m{}\033[00m'.format(args[0]))
    else:
        print('\033[95m{}\033[00m'.format(args))

def print_cyan(args):
    if isinstance(args, list):
        if len(args) == 2:
            print('{}\033[96m{}\033[00m'.format(args[0], args[1]))
        elif len(args) == 3:
            print('{}\033[96m{}\033[00m{}'.format(args[0], args[1], args[2]))
        elif len(args) == 1:
            print('\033[96m{}\033[00m'.format(args[0]))
    else:
        print('\033[96m{}\033[00m'.format(args))


def print_red(args):
    if isinstance(args, list):
        if len(args) == 2:
            print('{}\033[30;1;31m{}\033[00m'.format(args[0], args[1]))
        elif len(args) == 3:
            print('{}\033[30;1;31m{}\033[00m{}'.format(args[0], args[1], args[2]))
        elif len(args) == 1:
            print('\033[30;1;31m{}\033[00m'.format(args[0]))
    else:
        print('\033[30;1;31m{}\033[00m'.format(args))


def print_static(args):
    """
    Prints the arguments at the same place (printing does not scroll down the screen)
    :param args:
    :return:
    """
    sys.stdout.write('\r' + args)


def cv(im):
    # check if im is PIL Image and if so, convert it to numpy perform permutation and revert it back to PIL
    if isinstance(im, PIL.Image.Image):
        im2 = np.array(im)[..., ::-1]
        im = Image.fromarray(im2)
        return im
    elif im is None:
        return im
    else:
        return im[..., ::-1]


def show(im, title=None, resize=False, close=True):
    if resize:
        factor = 2
    else:
        factor = 1
    # if isinstance(im, PIL.Image.Image):
    #     im = np.array(im)
    if isinstance(im, np.ndarray) and len(im.shape) > 3:
        im = np.squeeze(im)
    if not isinstance(im, np.ndarray):
        # if resize:
        #     new_size = (int(x / 4) for x in im.size)
        #     im = im.resize(new_size)
        im = np.array(im)
    # elif resize:
    #     im = Image.fromarray(im)
    #     new_size = (int(x / 2) for x in im.size)
    #     im = im.resize(new_size)
    #     im = np.array(im)

    # calculate the size the figure needs to have to display the image in actual size
    dpi = matplotlib.rcParams['figure.dpi']  # by default is 100
    # height, width, depth = im.shape
    height, width = im.shape[:2]
    # What size does the figure need to be in inches to fit the image?
    figsize = width / (factor*float(dpi)), height / (factor*float(dpi))

    if not len(plt.get_fignums()):
        fig = plt.figure(figsize=figsize)
        ax = fig.add_axes([0, 0, 1, 1])
        # plt.figure()
    # if not close:
    #     plt.figure()
    # thismanager = plt.get_current_fig_manager()
    # form is widthxheight+x+y
    # im_geometry = '{}x{}+100+100'.format(im.shape[1], im.shape[0])
    # thismanager.window.wm_geometry(im_geometry)
    # plt.axis('tight')
    plt.axis('Off')
    # plt.autoscale(enable=True, tight=True)
    if title:
        if resize:
            title = '{} - resized/{}'.format(title, factor)
        # plt.title(title)
        fig = pylab.gcf()
        fig.canvas.set_window_title(title)
    if len(im.shape) == 2:
        plt.imshow(im, cmap='gray')
    else:
        plt.imshow(im)
    # plt.show()
    if close:
        plt.waitforbuttonpress()
        plt.close()


def display_image_in_actual_size(im_path):

    dpi = matplotlib.rcParams['figure.dpi']  # by default is 100
    im_data = plt.imread(im_path)
    height, width, depth = im_data.shape

    # What size does the figure need to be in inches to fit the image?
    figsize = width / float(dpi), height / float(dpi)

    # Create a figure of the right size with one axes that takes up the full figure
    fig = plt.figure(figsize=figsize)
    ax = fig.add_axes([0, 0, 1, 1])

    # Hide spines, ticks, etc.
    ax.axis('off')

    # Display the image.
    ax.imshow(im_data, cmap='gray')

    plt.show()


def show2(im, title=None):
    """
    It just displays an image using OpenCV
    :param im:
    :param title:
    :return:
    """
    if isinstance(im, PIL.Image.Image):
        im = np.array(im)
    if not title:
        title = 'image'
    cv2.imshow(title, im)
    cv2.waitKey(0)
    cv2.destroyAllWindows()


def show2_with_bbox(im_orig, bbox, title=None, bbox_method=1):
    """
    Display an image using opencv and drawing a (red) bbox around each object
    :param im:
    :param bbox: Should be in (x1, y1), (x2, y2) format
    :param title:
    :return:
    """
    if isinstance(im_orig, PIL.Image.Image):
        im_orig = np.array(im_orig)
    im = im_orig.copy()
    if isinstance(bbox, list):
        bbox = np.array(bbox)
    if bbox.shape == 1:
        bbox = np.expand_dims(bbox, 0)
    for i in range(bbox.shape[0]):
        (x1, y1, x2, y2) = [int(x) for x in bbox[i, :]]
        if bbox_method == 1:
            cv2.rectangle(im, (x1, y1), (x2, y2), (0, 0, 255), 2)
        elif bbox_method == 2:
            x2 += x1
            y2 += y1
            cv2.rectangle(im, (x1, y1), (x2, y2), (0, 0, 255), 2)
    if not title:
        title = 'image'
    cv2.imshow(title, im)
    cv2.moveWindow(title,0,0)
    cv2.waitKey(0)
    cv2.destroyAllWindows()


def show2_with_bboxes(im_orig, bbox1, bbox2, title=None, bbox_method=1):
    """
    Display an image using opencv and drawing 2 sets of bboxes:
        - One with red color around each object
        - One with blue color around each object
    :param im:
    :param bbox: Should be in (x1, y1), (x2, y2) format
    :param title:
    :return:
    """
    if isinstance(im_orig, PIL.Image.Image):
        im_orig = np.array(im_orig)
    im = im_orig.copy()
    if isinstance(bbox1, list):
        bbox1 = np.array(bbox1)
        bbox2 = np.array(bbox2)
    if bbox1.shape == 1:
        bbox1 = np.expand_dims(bbox1, 0)
    if bbox2.shape == 1:
        bbox2 = np.expand_dims(bbox2, 0)
    for i in range(bbox1.shape[0]):
        (x1, y1, x2, y2) = [int(x) for x in bbox1[i, :]]
        if bbox_method == 1:
            cv2.rectangle(im, (x1, y1), (x2, y2), (0, 0, 255), 2)
        elif bbox_method == 2:
            x2 += x1
            y2 += y1
            cv2.rectangle(im, (x1, y1), (x2, y2), (0, 0, 255), 2)
        # cv2.imwrite(f'./home/stayros/Desktop/isola_desktop/ode_reuslts/output_{i}.jpg', im)

    for i in range(bbox2.shape[0]):
        (x1, y1, x2, y2) = [int(x) for x in bbox2[i, :]]
        if bbox_method == 1:
            cv2.rectangle(im, (x1, y1), (x2, y2), (0, 255, 0), 2)
        elif bbox_method == 2:
            x2 += x1
            y2 += y1
            cv2.rectangle(im, (x1, y1), (x2, y2), (0, 255, 0), 2)
    if not title:
        title = 'image'
    cv2.imshow(title, im)
    cv2.waitKey(0)
    cv2.destroyAllWindows()


# def side_by_side(im1, im2, im_size=None, opencv_image=True, title=None):
#     """
#     Display 2 images side by side using OpenCV code
#     :param opencv_image:
#     :param im1:
#     :param im2:
#     :param im_size:
#     :return:
#     """
#     if not isinstance(im1, np.ndarray):
#         im1 = np.array(im1)
#     if not isinstance(im2, np.ndarray):
#         im2 = np.array(im2)
#     if im_size is None:
#         im1a = cv2.resize(im1, None, fx=0.8, fy=0.8)
#     else:
#         im1a = cv2.resize(im1, im_size)
#     if title is None:
#         title = 'side-by-side'
#     dim1a = im1a.shape
#     im2a = cv2.resize(im2, (dim1a[1], dim1a[0]))
#     dim2a = im2a.shape
#
#     # make the 2 images the same size
#     if len(dim1a) < len(dim2a):
#         im1a = np.asarray(np.dstack((im1a, im1a, im1a)), dtype=np.uint8).copy(order='C')
#     elif len(dim1a) > len(dim2a):
#         im2a = np.asarray(np.dstack((im2a, im2a, im2a)), dtype=np.uint8).copy(order='C')
#
#     # concatenate horizontally the 2 images and display them
#     final = np.hstack((im1a, im2a))
#     final = imt.image_to_255(final).astype(np.uint8)
#     if opencv_image:
#         show2(final, title)
#     else:
#         show(final, title, resize=False)


def display_bboxes(image_source, bboxes, labels=None, scores=None, tracks=None, plot=True, title=None, font_size=None):
    """
    Display the bboxes over the image using an array as parameter plus providing a label for the objects
    :param font_size:
    :param title:
    :param labels:
    :param bboxes:
    :param image_source:
    :param plot:
    :return:
    """
    if title is None:
        title = ''
    if labels is None:
        labels = []
        display_labels = False
    else:
        display_labels = True
        if not isinstance(labels, list):
            labels = [labels]
    if scores is None:
        scores = []
        display_scores = False
    else:
        display_scores = True
    if tracks is None:
        tracks = []
        display_tracks = False
    else:
        display_tracks = True

    path_label = False
    if isinstance(image_source, Image.Image):
        image = image_source
    elif isinstance(image_source, np.ndarray):
        image = Image.fromarray(image_source.astype('uint8'))
    elif os.path.exists(image_source):
        image = Image.open(image_source)
        path_label = True
    # colors = ['red', 'blue', 'green', 'black', 'orange',]
    colors = ['red', 'blue', 'green', 'orange', (128, 0, 255), (0, 128, 128), (200, 190, 0),
              (200, 64, 64), (165, 42, 42), (160, 128, 64)]
    color_id = 0
    class_color_dict = {}
    for i in range(bboxes.shape[0]):
        if display_labels:
            if display_scores:
                if not display_tracks:
                    display_str_list = ['{}: {:1.3f}'.format(labels[i], scores[i])]
                else:
                    display_str_list = ['{}: {:1.3f} tr{}'.format(labels[i], scores[i], tracks[i])]
            else:
                if display_tracks:
                    display_str_list = ['{}: tr{}'.format(labels[i], tracks[i])]
                else:
                    display_str_list = [labels[i]]
            if labels[i] in class_color_dict:
                color = class_color_dict[labels[i]]
            else:
                class_color_dict[labels[i]] = colors[color_id % len(colors)]
                color = colors[color_id % len(colors)]
                color_id += 1
        else:
            display_str_list = ['']
            color = 'red'
        # print('Color: {}, color_id={}, label={}'.format(color, color_id, display_str_list[0]))
        draw_bounding_box_on_image(image, bboxes[i, 1], bboxes[i, 0], bboxes[i, 3], bboxes[i, 2],
                                   color=color, thickness=2, display_str_list=display_str_list,
                                   use_normalized_coordinates=False, font_size=font_size)

    if plot:
        if path_label and not title:
            plt.figure(os.path.basename(image_source), figsize=(16, 12), dpi=80)
        else:
            plt.figure(title, figsize=(16, 12), dpi=80)
        plt.imshow(image)
        plt.waitforbuttonpress()
        plt.close()
    return image


def draw_bounding_box_on_image(image, ymin, xmin, ymax, xmax, color='red', thickness=4,
                               display_str_list=(), use_normalized_coordinates=True, font_size=None):
    """Adds a bounding box to an image.

  Bounding box coordinates can be specified in either absolute (pixel) or
  normalized coordinates by setting the use_normalized_coordinates argument.

  Each string in display_str_list is displayed on a separate line above the
  bounding box in black text on a rectangle filled with the input 'color'.
  If the top of the bounding box extends to the edge of the image, the strings
  are displayed below the bounding box.

  Args:
    image: a PIL.Image object.
    ymin: ymin of bounding box.
    xmin: xmin of bounding box.
    ymax: ymax of bounding box.
    xmax: xmax of bounding box.
    color: color to draw bounding box. Default is red.
    thickness: line thickness. Default value is 4.
    display_str_list: list of strings to display in box
                      (each to be shown on its own line).
    use_normalized_coordinates: If True (default), treat coordinates
      ymin, xmin, ymax, xmax as relative to the image.  Otherwise treat
      coordinates as absolute.
      :param display_str_list:
      :param use_normalized_coordinates:
      :param font_size:
  """
    if isinstance(font_size, str):
        if font_size == "tiny":
            font_size = 8
        elif font_size == "small":
            font_size = 12
        elif font_size == "normal":
            font_size = 24
        elif font_size == "large":
            font_size = 36
    elif font_size is None:
        font_size = 24
    draw = ImageDraw.Draw(image)
    im_width, im_height = image.size
    if use_normalized_coordinates:
        (left, right, top, bottom) = (xmin * im_width, xmax * im_width,
                                      ymin * im_height, ymax * im_height)
    else:
        (left, right, top, bottom) = (xmin, xmax, ymin, ymax)
    draw.line([(left, top), (left, bottom), (right, bottom),
               (right, top), (left, top)], width=thickness, fill=color)
    try:
        font = ImageFont.truetype('arial.ttf', font_size)
    except IOError:
        font = ImageFont.load_default()

    # If the total height of the display strings added to the top of the bounding
    # box exceeds the top of the image, stack the strings below the bounding box
    # instead of above.
    display_str_heights = [font.getsize(ds)[1] for ds in display_str_list]
    # Each display_str has a top and bottom margin of 0.05x.
    total_display_str_height = (1 + 2 * 0.05) * sum(display_str_heights)

    if top > total_display_str_height:
        text_bottom = top
    else:
        text_bottom = bottom + total_display_str_height
    # Reverse list and print from bottom to top.
    for display_str in display_str_list[::-1]:
        text_width, text_height = font.getsize(display_str)
        margin = np.ceil(0.05 * text_height)
        draw.rectangle(
            [(left, text_bottom - text_height - 2 * margin), (left + text_width,
                                                              text_bottom)],
            fill=color)
        draw.text(
            (left + margin, text_bottom - text_height - margin),
            display_str,
            fill='black',
            font=font)
        text_bottom -= text_height - 2 * margin


def display_images_from_file(image_list_path, resize=False):
    """
    Just displaying recursively all images contained in a list in a text file
    :param image_list_path:
    :return:
    """
    with open(image_list_path) as fp:
        for line in fp:
            line = line.strip()
            im = Image.open(line)
            title = os.path.basename(line)
            show(im, title=title, resize=resize)


def main():
    image_list_path = '/home/gorfanidis/Datasets/KITTI/training/img_train2.txt'
    display_images_from_file(image_list_path, resize=False)


if __name__ == '__main__':
    main()
