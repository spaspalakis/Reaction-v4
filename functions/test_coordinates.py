from shapely.geometry import Point, Polygon

# Define polygon as list of (lon, lat) tuples
POLYGON_COORDS = [
    (23.74843805859978, 37.97638404818811),
    (23.79031809636623, 37.97381302651111),
    (23.7678333219916, 37.96339270089696),
    (23.74843805859978, 37.97638404818811)
]
POLYGON = Polygon(POLYGON_COORDS)

# Test coordinates
test_coords = [
    (23.73, 37.97),  # OUTSIDE_POSITION_1
    (23.76, 37.97),  # INSIDE_POSITION
    (23.80, 37.97),  # OUTSIDE_POSITION_2
]

print("Testing coordinates against polygon:")
for lon, lat in test_coords:
    point = Point(lon, lat)
    is_inside = POLYGON.contains(point)
    print(f"Point ({lon}, {lat}): {'INSIDE' if is_inside else 'OUTSIDE'} the polygon") 