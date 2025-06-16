from functions import display_tools as dt
import numpy as np
import cv2 
import os
import time



def save_image(output_img,im_path):
    
    write_on_rgb = cv2.cvtColor(output_img, cv2.COLOR_BGR2RGB)                
    cv2.imwrite(im_path,  cv2.cvtColor(write_on_rgb, cv2.COLOR_BGR2RGB))
    return



def frame_rate(image,prev_frame_time,new_frame_time):

            # font which we will be using to display FPS
            font = cv2.FONT_HERSHEY_SIMPLEX
            # time when we finish processing for this frame
            new_frame_time = time.time()

            # fps will be number of frame processed in given time frame
            # since their will be most of time error of 0.001 second
            # we will be subtracting it to get more accurate result
            fps2 = 1/(new_frame_time-prev_frame_time)
            prev_frame_time = new_frame_time

            # converting the fps into integer
            fps2 = int(fps2)

            # converting the fps to string so that we can display it on frame
            # by using putText function
            fps2 = str(fps2)

            # putting the FPS count on the frame
            cv2.putText(image, fps2, (7, 70), font, 3, (100, 255, 0), 3, cv2.LINE_AA)





####---------------- unused ---------------####




# def frame_with_bbox(image,boxes,fr):  
def plot_with_bbox(image, boxes, fr):
                        
        mid_points = (boxes[:, 0] + boxes[:, 2]) / 2 #; print(f'\nboxes in the plot: {boxes}')
        mx = y = 0
        for k in range(mid_points.shape[0]):
                mx = round(mid_points[k]) #; print(f'\nmx: {mx}')
                y = round(boxes[k, 3]) #; print(f'y: {y}    

        if mx != 0 and y != 0:      
                dt.show2_with_bbox(image, boxes, title='frame: {}'.format(fr))
        

# def snapshot_with_bbox(image, bboxes, im_path):

#     # Create a copy of the image to draw the boxes on
#     image_with_boxes = image.copy()

#     # Draw bounding boxes on the image
#     for bbox in bboxes:
#         x1, y1, x2, y2 = bbox
#         x1, y1, x2, y2 = map(int, [x1, y1, x2, y2])
#         cv2.rectangle(image_with_boxes, (x1, y1), (x2, y2), (0,0,255), 2)  # green :(0, 255, 0)


#     # Save the image with bounding boxes to the specified 'im_path'
#     cv2.imwrite(im_path, image_with_boxes)
    
#     return image_with_boxes
                

def save_with_bbox(save_frame,image, bboxes, classes, scores,im_path, class_names, class_colors):
    
    image_with_boxes = image.copy()
    
    # Draw bounding boxes on the image
    for i, bbox in enumerate(bboxes):
        x1, y1, x2, y2 = map(int, bbox)
        class_index = int(classes[i])  # Convert numeric class label to integer
        class_name = class_names[class_index]  # Get class name from class_names list
        score = float(scores[i])

        # Draw bounding box rectangle with class-specific color
        color = class_colors[class_name]
        cv2.rectangle(image_with_boxes, (x1, y1), (x2, y2), color, 2)

        # Write class name and score on the image
        text = f"{class_name}: {score:.2f}"
        cv2.putText(image_with_boxes, text, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)


    #Save the image with bounding boxes to the specified 'im_path'
    if save_frame:
        cv2.imwrite(im_path, image_with_boxes)
    
    
    return image_with_boxes


