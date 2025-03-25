import json
import cv2
import av
import argparse
import pytesseract
import logging
from datetime import datetime
import time

from objects.boiler import BoilerData
from persistence.database import MariaDBHandler

CAMERA_CONNECTION_ATTEMPTS_LIMIT = 3
DEFAULT_WAIT_TIME_IN_SECONDS = 1

def form_database_connection(user : str, pwd : str, host : str, db : str):
    database_url = f"mariadb+mariadbconnector://{user}:{pwd}@{host}/{db}"
    return database_url
def process_image(image, is_debug):
    gray_frame = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray_frame,(13,13),0)    
    image_to_test = blur 
    #cv2.threshold(gray_frame, 150, 255, cv2.THRESH_BINARY)
    if is_debug == True:
        cv2.namedWindow("Debug window", cv2.WINDOW_NORMAL)
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
    wait_time = DEFAULT_WAIT_TIME_IN_SECONDS if "wait" not in app_settings["app"] else app_settings["app"]["wait"]

    # Capture video from a specified source (default is webcam)
    main_logger.info("Video feed started. Analyzing frames.")
    connection_attempts = 0
    boiler_is_disabled = 0

    db_handler = None
    if args.dry_run == False:
        db_handler = MariaDBHandler(database_url, main_logger)
    
    main_logger.debug(f"Trying to connect to {source}")
    main_logger.info("Starting video capture")    
    start_time = time.time()
    container = av.open(source, format="rtsp", timeout=5)
    
    try:
        for idx, frame in enumerate(container.decode(video=0)):
            main_logger.debug(f"Frame {idx}: {frame.width}x{frame.height} at {frame.time}s")
            if connection_attempts > CAMERA_CONNECTION_ATTEMPTS_LIMIT:
                main_logger.critical("Couldn't open video feed. Giving up.")
                return            
            # Extract text from the current frame
            detected_text = extract_text(frame.to_ndarray(format="bgr24"), args.debug)
            
            # Parse the detected text (this is a basic example)
            main_logger.debug(f"Detected Text: {detected_text}")
            result = None
            
            try:    
                result = BoilerData(detected_text, main_logger, args.dry_run, db_handler)
                if result.is_burning == True:
                    boiler_is_disabled = 0
                    result.persist_run()
                
                elif result.is_burning == False and boiler_is_disabled == 0:
                    boiler_is_disabled += 1
                    main_logger.info("Boiler is marked as disabled. Next run won't persist.")
                    result.persist_run()
                    
            except Exception as e:
                main_logger.warning(f"Failed while forming the log. Retrying in the next cycle {e}. OCR is {detected_text}")
            
            time.sleep(wait_time)
            main_logger.debug("Next frame")

    except KeyboardInterrupt:
        container.close()
        cv2.destroyAllWindows()

        main_logger.debug("All video windows destroyed")
        main_logger.info("Finished capture.")
        return
if __name__ == "__main__":
    main()
