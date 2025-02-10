import json
import cv2
import argparse
import pytesseract
import logging
from datetime import datetime
import time
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from objects.boiler import BoilerData

def form_database_connection(user : str, pwd : str, host : str, db : str):
    database_url = f"mysql+pymysql://{user}:{pwd}@{host}/{db}"
    engine = create_engine(database_url)
    SessionLocal = sessionmaker(bind=engine)

    return SessionLocal
def process_image(image):
    gray_frame = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    ret, image_to_test = cv2.threshold(gray_frame, 150, 255, cv2.THRESH_BINARY)
    return image_to_test
def form_source_endpoint(ip : str, port : str) -> str:
    endpoint = f"rtsp://{ip}:{port}/h264.sdp"
    return endpoint
def get_settings(file_name : str) -> dict:
    settings = None
    with open(file_name, 'r') as file:
        settings = json.load(file)
    return settings
def extract_text(frame):
    #cv2.imshow("Debug window", image_to_test)
    # Use pytesseract to do OCR on the processed frame
    image_to_parse = process_image(frame) 
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
    source = form_source_endpoint(app_settings["connection"]["ip"], app_settings["connection"]["port"])
    wait_time = 0 if "wait" not in app_settings["app"] else app_settings["app"]["wait"]

    # Capture video from a specified source (default is webcam)
    try:
        capture = cv2.VideoCapture(source)
    
        main_logger.info("Starting video capture")

        if not capture.isOpened():
            main_logger.critical("Couldn't open video feed")
            return
        
    except Exception:
        main_logger.critical(f"Couldn't read video feed")
        return
    
    feed_live = True
    main_logger.info("Video feed started. Analyzing frames.")
    while feed_live:
        ret, frame = capture.read()
        if not ret:
            main_logger.critical("Couldn't read frame.")
            break
        # Extract text from the current frame
        detected_text = extract_text(frame)
        
        # Parse the detected text (this is a basic example)
        main_logger.debug(f"Detected Text: {detected_text}")
        with form_database_connection() as session:
            new_record = record_crud.create(session, column_name="value")  # Replace with actual column names
            fetched_record = record_crud.read(session, obj_id=1)

            result = BoilerData(detected_text, main_logger, args.dry_run)
            result.persist_run()
        
        time.sleep(wait_time)
        # Release resources
    capture.release()
    main_logger.debug("Released capture")
    cv2.destroyAllWindows()
    main_logger.debug("All video windows destroyed")
    main_logger.info("Finished capture.")
if __name__ == "__main__":
    main()