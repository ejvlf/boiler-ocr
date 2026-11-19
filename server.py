from pathlib import Path
from flask import Flask, request, jsonify
import os
import numpy as np
import pytesseract
from datetime import datetime
import cv2

from objects.applications import form_logger, form_database_connection, get_settings
from objects.boiler import BoilerData
from persistence.database import MariaDBHandler

UPLOAD_FOLDER = "/tmp/captures"

is_debug = os.environ.get("FLASK_DEBUG", False)
app = Flask(__name__)
base_dir = Path(__file__).resolve().parent

app_logger = form_logger(is_debug, False, "server")

app_settings = get_settings(f"settings.json")
database_url = form_database_connection(
                    app_settings["app"]["database"]["user"],
                    app_settings["app"]["database"]["password"],
                    app_settings["app"]["database"]["host"],
                    app_settings["app"]["database"]["database"]
                )

app_logger.info("Starting application...")
app_logger.debug(f"Base path is {base_dir.as_uri()}")

db_handler = MariaDBHandler(database_url, app_logger)
pytesseract.pytesseract.tesseract_cmd = app_settings["ocr"]["tesseract-dir"]

previous_record = None
stop_recording = False

def process_image(image, is_debug: bool):
    gray_frame = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    _, image_to_test = cv2.threshold(gray_frame, 230, 200, cv2.THRESH_BINARY_INV)
    if is_debug:
        # Only touch the disk when you're actually debugging
        Path(UPLOAD_FOLDER).mkdir(parents=True, exist_ok=True)
        name = f"{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_processed.jpg"
        cv2.imwrite((Path(UPLOAD_FOLDER) / name).as_posix(), image_to_test)
    return image_to_test

def extract_text(frame, is_debug: bool):
    image_to_parse = process_image(frame, is_debug)
    text = pytesseract.image_to_string(
        image_to_parse, lang='lets',
        config="--oem 3 --psm 6 -c tessedit_char_whitelist=aA1234567890"
    )
    return text.strip()

@app.route("/reading", methods=["POST"])
def receive_image():
    global previous_record, stop_recording

    if "file" not in request.files:
        return jsonify({"error": "no file"}), 400

    detected_text = None
    try:
        data = np.frombuffer(request.files["file"].read(), dtype=np.uint8)
        frame = cv2.imdecode(data, cv2.IMREAD_COLOR)
        if frame is None:
            return jsonify({"error": "invalid image"}), 400

        detected_text = extract_text(frame, is_debug)
        # ... rest of your logic unchanged
    except Exception as e:
        app_logger.warning(f"Failed while forming the log: {e}. OCR is {detected_text}")
        return jsonify({"status": f"failed {e}"}), 500

    return jsonify({"status": "ok"}), 200