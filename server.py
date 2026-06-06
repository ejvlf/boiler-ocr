from pathlib import Path
from flask import Flask, request, jsonify
import os
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

def process_image(image, is_debug : bool, original_path : Path):
    gray_frame = cv2.cvtColor(image, cv2.COLOR_RGBA2GRAY)
    ret, image_to_test = cv2.threshold(gray_frame, 230, 200, cv2.THRESH_BINARY_INV)    
    if is_debug == True:
        cv2.imshow("Debug window", image_to_test)
        cv2.waitKey(0)
    
    new_filename = f"{original_path.name}_processed.jpg"
    new_filepath = Path(UPLOAD_FOLDER) / new_filename
    cv2.imwrite(new_filepath.as_posix(), image_to_test)
    return image_to_test

def extract_text(frame, is_debug, original_path : Path):
    image_to_parse = process_image(frame, is_debug, original_path)
    text = pytesseract.image_to_string(image_to_parse, lang='lets', config="--oem 3 --psm 6 -c tessedit_char_whitelist=aA1234567890")
    return text.strip()

@app.route("/reading", methods=["POST"])
def receive_image():
    global previous_record, stop_recording
    if "file" not in request.files:
        return jsonify({"error": "no file"}), 400

    file = request.files["file"]

    Path(UPLOAD_FOLDER).mkdir(parents=True, exist_ok=True)
    filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
    filepath = Path(UPLOAD_FOLDER) / filename
    file.save(filepath)
    result = None

    try:
        frame = cv2.imread(filepath)
        # Tesseract analisa a imagem e transforma numa string
        detected_text = extract_text(frame, is_debug, filepath)
        app_logger.debug(f"Detected Text: {detected_text}")

        # Instanciar. Validações estão dentro do objeto                
        result = BoilerData(detected_text, app_logger, False, db_handler)

        if previous_record is not None and (result.is_burning == previous_record.is_burning and 
            result.temperature == previous_record.temperature and  
            result.running_mode == previous_record.running_mode):
            app_logger.debug("No significant change detected. Not persisting.")

        if result.is_burning == True and stop_recording == True:
            app_logger.info("Boiler is on again. Resuming persistence.")
            
        if result.is_valid == True and stop_recording == False:
            result.persist_run()
            previous_record = result
        
        if result.is_burning == False and stop_recording == False:
            app_logger.info("Boiler is off. Not persisting until turned on.")


    except Exception as e:
        app_logger.warning(f"Failed while forming the log. Retrying in the next cycle {e}. OCR is {detected_text}")
        return jsonify({"status": "failed {e}"}), 500

    return jsonify({"status": "ok"}), 200