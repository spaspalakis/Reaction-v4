from flask import Flask, send_from_directory, abort
from flask import render_template
import os, json

app = Flask(__name__)

# The directory where your frames are stored
IMAGES_DIR = "/home/stavros/Documents/certh/projects/REACTION/codes/Reaction-v2/output/frames"


@app.route('/images/<path:filename>')
def get_image(filename):
    # Security: Prevent accessing files outside the directory
    if ".." in filename or filename.startswith("/"):
        abort(400, description="Invalid filename.")

    # Serve the file if it exists
    if os.path.isfile(os.path.join(IMAGES_DIR, filename)):        
        return send_from_directory(IMAGES_DIR, filename)
    else:
        abort(404, description="Image not found.")

if __name__ == "__main__":
    # Run on port 8000 by default
    app.run(host="0.0.0.0", port=8000)

