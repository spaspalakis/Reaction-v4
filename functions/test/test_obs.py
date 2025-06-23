# import cv2

# # RTMP URL from OBS
# stream_url = "rtmp://127.0.0.1:1935/live/mystream"

# cap = cv2.VideoCapture(stream_url)

# while True:
#     ret, frame = cap.read()
#     if not ret:
#         print("Failed to grab frame")
#         break

#     cv2.imshow("Stream", frame)

#     if cv2.waitKey(1) & 0xFF == ord('q'):
#         break

# cap.release()
# cv2.destroyAllWindows()

import cv2

cap = cv2.VideoCapture(2)  # Try 1 or 2 if 0 doesn't work

while True:
    ret, frame = cap.read()
    if not ret:
        break
    cv2.imshow("OBS Virtual Camera", frame)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cap.release()
cv2.destroyAllWindows()