import json
import cv2
import argparse
import pytesseract
import logging
from datetime import datetime
import time

from objects.boiler import BoilerData
from persistence.database import MariaDBHandler

CAMERA_CONNECTION_ATTEMPTS_LIMIT = 3
TEMPERATURE_THRESHOLD = 10

def form_database_connection(user : str, pwd : str, host : str, db : str):
    database_url = f"mariadb+mariadbconnector://{user}:{pwd}@{host}/{db}"
    return database_url
def process_image(image, is_debug):
    gray_frame = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
    ret, image_to_test = cv2.threshold(gray_frame, 200, 255, cv2.THRESH_BINARY_INV)    
    if is_debug == True:
        cv2.imshow("Debug window", image_to_test)
        cv2.waitKey(0)

    return image_to_test
def form_source_endpoint(ip : str, port : str) -> str:
    endpoint = f"rtsp://{ip}:{port}/h264.sdp"
    return endpoint
def get_settings(file_name : str) -> dict:
    settings = None
    with open(file_name, 'r') as file:
        settings = json.load(file)
    return settings
def extract_text(frame, is_debug):
    # Use pytesseract to do OCR on the processed frame
    image_to_parse = process_image(frame, is_debug) 
    text = pytesseract.image_to_string(image_to_parse, lang='lets', config="--oem 3 --psm 6 -c tessedit_char_whitelist=A1234567890")
    return text.strip()
def main():    
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", help="add more logging information to application", action="store_true")
    parser.add_argument("--file_log", help="Log actions to a file", action="store_true")
    parser.add_argument("--dry_run", help="Only log results", action="store_true")
    parser.add_argument("--settings", help="Name of the file with the settings. Needs to be json.")

    args = parser.parse_args()
    
    app_settings = get_settings(f"{args.settings}.json")
    # set tesseract dir
    pytesseract.pytesseract.tesseract_cmd = app_settings["ocr"]["tesseract-dir"]
    
    # Logging info
    logging_level = logging.DEBUG if args.debug == True else logging.INFO
    main_logger = logging.getLogger("Boiler OCR")    
    
    if args.file_log == True:
        fname = f"{datetime.now().date().strftime('%Y-%m-%d')}_boiler_ocr.log"
        logging.basicConfig(filename=fname, level=logging_level, format=f'%(asctime)s %(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging_level, format=f'%(asctime)s %(levelname)s: %(message)s') 
    
    # create source video feed
    source = form_source_endpoint(app_settings["camera"]["connection"]["ip"], app_settings["camera"]["connection"]["port"])
    database_url = form_database_connection(app_settings["app"]["database"]["user"],
                                            app_settings["app"]["database"]["password"],
                                            app_settings["app"]["database"]["host"],
                                            app_settings["app"]["database"]["database"]
                                            )
    wait_time = 0 if "wait" not in app_settings["app"] else app_settings["app"]["wait"]

    # Capture video from a specified source (default is webcam)
    feed_live = True
    main_logger.info("Video feed started. Analyzing frames.")
    connection_attempts = 0

    db_handler = None
    if args.dry_run == False:
        db_handler = MariaDBHandler(database_url, main_logger)
    
    main_logger.debug(f"Trying to connect to {source}")

    try:
        while feed_live:
            capture = cv2.VideoCapture(source, cv2.CAP_FFMPEG)
            main_logger.debug("Reading frame")

            if not capture.isOpened():
                capture.release()
                connection_attempts += 1
                if connection_attempts > CAMERA_CONNECTION_ATTEMPTS_LIMIT:
                    main_logger.critical("Couldn't open video feed. Giving up.")
                    return                    

                main_logger.warning(f"Couldn't open video feed. Retrying: {connection_attempts}")
                time.sleep(5)
                capture = cv2.VideoCapture(source, cv2.CAP_FFMPEG)
                capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)                
                continue

            ret, frame = capture.read()
            if not ret:
                main_logger.critical("Couldn't read frame.")
                return
            
            # Extract text from the current frame
            detected_text = extract_text(frame, args.debug)
            
            # Parse the detected text (this is a basic example)
            main_logger.debug(f"Detected Text: {detected_text}")
            result = None
            try:
                
                result = BoilerData(detected_text, main_logger, args.dry_run, db_handler)
                
                if result.temperature > TEMPERATURE_THRESHOLD:
                    result.persist_run()
                    
            except Exception as e:
                main_logger.warning(f"Failed while forming the log. Retrying in the next cycle {e}. OCR is {detected_text}")
            
            capture.release()
            cv2.destroyAllWindows()
            main_logger.debug("Resources released. Waiting")
            time.sleep(wait_time)

    except KeyboardInterrupt:
        capture.release()
        cv2.destroyAllWindows()

        main_logger.debug("All video windows destroyed")
        main_logger.info("Finished capture.")
        return
if __name__ == "__main__":
    main()