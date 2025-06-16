from functions import display_tools as dt
import numpy as np

def filter_boxes(final_track_ids,
                 boxes,
                 labels,
                 scores):


    """
    Filter the final_track_ids list. Only the the objects who have trackID are displayed.
    So if an object is detectgives by defalault ID=0 as a newcomer in the frame.
    If the object is detected in 3 consecutive frames, then the tracker gives an ascending ID to this object
    """            
    valid_indices = [i for i, track_id in enumerate(final_track_ids) if track_id != 0]
    filtered_boxes = boxes[valid_indices]
    filtered_labels = [labels[i] for i in valid_indices]
    filtered_scores = [scores[i] for i in valid_indices]
    filtered_track_ids = [final_track_ids[i] for i in valid_indices]

    return filtered_boxes,filtered_labels,filtered_scores,filtered_track_ids