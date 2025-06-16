import tensorflow as tf
import numpy as np

# from functions import xywh2xyxy as xy

def xywh2xyxy(x):
    # Convert nx4 boxes from [x, y, w, h] to [x1, y1, x2, y2] where xy1=top-left, xy2=bottom-right
    y = np.copy(x) if isinstance(x, np.ndarray) else x.clone()
    # y = x.clone() if isinstance(x, torch.Tensor) else np.copy(x)
    y[..., 0] = x[..., 0] - x[..., 2] / 2  # top left x
    y[..., 1] = x[..., 1] - x[..., 3] / 2  # top left y
    y[..., 2] = x[..., 0] + x[..., 2] / 2  # bottom right x
    y[..., 3] = x[..., 1] + x[..., 3] / 2  # bottom right y
    return y


def nms_tf(pred_bbox, w, h, model_dim, overlap_thres=0.45, score_thres=0.25):
    """
    Yolov8 has some difference over yolov5.
    The bboxes dimensions are in absolute coordinates (over the model image input of course)
    The pred_bbox is (batch_size, num_detections, 4+num_cless) for yolov8 while for
    yolov5 was (batch_size, 4+num_classes+1, num_detections)
    Yolov8: bboxes are the first 4 columns, the others being the conf for the classes
    :param pred_bbox:
    :param w:
    :param h:
    :param model_dim:
    :param overlap_thres:
    :param score_thres:
    :return:
    """
    pred_bbox = pred_bbox.numpy()
    pred_bbox = np.swapaxes(pred_bbox, 1, 2) # Swaps the 2nd and 3rd axes of pred_bbox for easier manipulation.

    # pred_bbox = pred_bbox['output_0'].numpy()
    # overlap_thres = 0.45  # 0.2  #
    # score_thres = 0.25  # 0.2  #
    # boxes = pred_bbox[:, :, 0:4]

    #Extracts the bounding boxes and converts them from [x, y, w, h] format to [x1, y1, x2, y2] format using xywh2xyxy.
    boxes = pred_bbox[:, :, 0:4]
    boxes = xywh2xyxy(boxes[:, :])  # convert to xyxy

    #Extracts the confidence scores for the classes.
    #Normalizes the bounding boxes by dividing by the maximum coordinate value.
    pred_conf = pred_bbox[:, :, 4:]
    max_coord = np.max(boxes)
    boxes = boxes/max_coord

    #Applies combined non-max suppression using TensorFlow's combined_non_max_suppression function.
    #This filters overlapping boxes based on IoU and score thresholds.
    boxes, scores, classes, valid_detections = tf.image.combined_non_max_suppression(
        boxes=tf.reshape(boxes, (tf.shape(boxes)[0], -1, 1, 4)),
        scores=tf.reshape(
            pred_conf, (tf.shape(pred_conf)[0], -1, tf.shape(pred_conf)[-1])),
        max_output_size_per_class=50,
        max_total_size=50,
        iou_threshold=overlap_thres,  
        score_threshold=score_thres)   # this filters all values < SCORE_THRES
    
    
    #Converts the boxes back to NumPy arrays and scales them back to the original dimensions.
    boxes = boxes.numpy()
    boxes = boxes*max_coord
    # num_objects = valid_detections.numpy()[0]
    # print(f"\n### num_objects: {num_objects} | classes: {classes} | scores: {scores} ")

    #Removes single-dimensional entries from the shape of the boxes, scores, and classes arrays.
    boxes = np.squeeze(boxes)
    scores = np.squeeze(scores)
    classes = np.squeeze(classes)

    #Filters out detections with scores below the threshold. Normalizes the boxes with respect to the model dimensions.
    inds = scores > score_thres
    boxes = boxes[inds]
    classes = classes[inds]
    scores = scores[inds]
    boxes = boxes/model_dim

    #Adjusts the bounding boxes to account for differences in aspect ratio between the original image and the model input dimensions.
    if w > h:
        boxes[:, [1, 3]] = boxes[:, [1, 3]] * w / h
        boxes[:, [1, 3]] = boxes[:, [1, 3]] - (w - h) / 2 / h
    else:
        boxes[:, [0, 2]] = boxes[:, [0, 2]] * h / w
        boxes[:, [0, 2]] = boxes[:, [0, 2]] - (h - w) / 2 / w
    # boxes[:, [1]] = boxes[:, [1]] - 57/286  # 400
    boxes[..., :4] *= [w, h, w, h]  # xywh normalized to pixels

    return boxes, classes, scores,valid_detections