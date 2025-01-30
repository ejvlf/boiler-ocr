import json
import cv2
import argparse
import cv2
import pytesseract
import logging
from datetime import datetime
import time

class BoilerData:
    def __init__(self, raw_data, logger, is_dry_run):
        self.log = logger
        self.is_burning = self._form_is_burning(raw_data)
        self.temperature = self._form_temperature(raw_data)
        self.marked_time = self._form_marked_time(raw_data)
        self.running_mode = self._form_running_mode(raw_data)
        self.dry_run = is_dry_run
    def _form_is_burning(self, raw_data):
        is_burning = False            
        if len(raw_data) > 7:
            is_burning = True
        return is_burning
    def _form_marked_time (self, raw_data):
        time_as_string = raw_data.splitlines()[0].strip()
        time_as_datetime = None
        try:
            if len(time_as_string) == 4 and time_as_string.find(" ") == -1:
                return datetime(datetime.now().year, datetime.now().month, datetime.now().day, int(time_as_string[0:2]), int(time_as_string[2:4]), datetime.now().second)
            elif len(time_as_string) == 5 and time_as_string.find(" ") > -1:
                return datetime(datetime.now().year, datetime.now().month, datetime.now().day, int(time_as_string[0:2]), int(time_as_string[3:5]), datetime.now().second)
            elif len(time_as_string) == 5 and time_as_string.find(" ") == -1:
                return datetime(datetime.now().year, datetime.now().month, datetime.now().day, int(time_as_string[1:3]), int(time_as_string[3:5]), datetime.now().second)
            elif len(time_as_string) == 6 and time_as_string.find(" ") > -1:
                return datetime(datetime.now().year, datetime.now().month, datetime.now().day, int(time_as_string[1:2]), int(time_as_string[5:6]), datetime.now().second)
            elif len(time_as_string) == 6 and time_as_string.find(" ") == -1:
                return datetime(datetime.now().year, datetime.now().month, datetime.now().day, int(time_as_string[1:2]), int(time_as_string[5:6]), datetime.now().second)
            else:
                self.log.warning(f"Couldn't form date given {time_as_string}. Assuming current datetime")
        except ValueError:
            self.log.warning(f"Wrong value from OCR {time_as_string}. Assuming current datetime")

        if time_as_datetime is None:
            time_as_datetime = datetime.now()
        return time_as_datetime
    def _form_running_mode (self, raw_data) -> str:
        running_mode = "0"
        if self.is_burning:
            mode_from_string = raw_data.splitlines()[1]
            running_mode = mode_from_string[0:mode_from_string.find(" ")].strip()
        return running_mode
    def _form_temperature(self, raw_data):
        temperature_to_return = 0
        temperature_as_string = raw_data.splitlines()
        if self.is_burning:
            empty_space_idx = temperature_as_string[1].find(" ")
            temperature_to_return = int(temperature_as_string[1][empty_space_idx:].strip())
        else:
            temperature_to_return = int(temperature_as_string[1])
        return temperature_to_return
        
    def persist_run(self):
        if self.dry_run:
            burning = "No" if self.is_burning == False else "Yes"
            self.log.info(f"Current status: Marked time - {self.marked_time}|Temperature - {self.temperature}|Running mode - {self.running_mode}|Burning - {burning}")


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
        result = BoilerData(detected_text, main_logger, True)
        result.persist_run()
        
        if cv2.waitKey(10) == 27:
            feed_live = False
        time.sleep(wait_time)
        # Release resources
    capture.release()
    main_logger.debug("Released capture")
    cv2.destroyAllWindows()
    main_logger.debug("All video windows destroyed")
    main_logger.info("Finished capture.")
if __name__ == "__main__":
    main()