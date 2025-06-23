    #####################
    ### Example usage ###
    #####################

    # pixel = (640, 360)  # Detected object coordinates in pixels
    # img_size = (1280, 720)  # Image dimensions
    # fov = (68, 40)  # Camera field of view in degrees (width, height)
    # drone_info = (35.7749, 33.4194, 100, 90, 0)  # Drone telemetry data (lat, lon, alt, bearing, pitch)
                                                        #bearing: The drone's heading (compass direction) in degrees, typically measured clockwise from North.
                                                        #pitch: The camera's pitch angle in degrees (positive usually means looking downwards, negative upwards).
    # gps_coords = pixel_to_gps(pixel, img_size, fov, drone_info)
    # print(gps_coords)  # Output: (latitude, longitude)

import math 


def pixel_to_gps(pixel, img_size, fov, drone_info):

        lat, lon, alt, bearing, pitch = drone_info # Unpack drone_info

        # Calculate the angle of the pixel from the center of the image
        dx = (pixel[0] - img_size[0] / 2) / (img_size[0] / 2) * (fov[0] / 2)
        dy = (
            ((img_size[1] - pixel[1]) - img_size[1] / 2)
            / (img_size[1] / 2)
            * (fov[1] / 2)
        )

        dy += pitch # Adjust the angles for the pitch of the camera. The camera's pitch angle in degrees (positive usually means looking downwards, negative upwards).

        # Calculate the relative position of the pixel from the drone (Ground Distance)
        dx = alt * math.tan(math.radians(dx))
        dy = alt * math.tan(math.radians(dy))

        # Rotate the relative position by the drone's bearing
        dx, dy = dx * math.cos(math.radians(-bearing)) - dy * math.sin(
            math.radians(-bearing)
        ), dx * math.sin(math.radians(-bearing)) + dy * math.cos(math.radians(-bearing))

        # Convert the relative position to GPS coordinates
        lat += dy / 111111
        lon += dx / (111111 * math.cos(math.radians(lat)))

        return lat, lon
 