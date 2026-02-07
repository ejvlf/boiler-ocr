import json
import cv2
import argparse
import pytesseract
import logging
import signal
from datetime import datetime
import time

from objects.analytics import ReportProcessor
from objects.boiler import BoilerData
from persistence.database import MariaDBHandler

CAMERA_CONNECTION_ATTEMPTS_LIMIT = 3

"""

Aplicação gloriosa que lê o leitor das caldeiras Ferlux e 
transforma a informação do visor em extraordinarios dados. 

Usa CV2 e Tesseract OCR para operar a magia sobre a imagem e 
depois inserir esses dados numa base de dados. Bastante inutil mas um exercicio divertido. 

"""

def form_logger(is_debug: bool, is_file_log: bool, mod_name: str) -> logging.Logger:

    logging_level = logging.DEBUG if is_debug else logging.INFO
    
    # Create logger
    logger = logging.getLogger(f"Boiler OCR - {mod_name}")
    logger.setLevel(logging_level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    
    # Create appropriate handler
    if is_file_log:
        fname = f"{datetime.now().date().strftime('%Y-%m-%d')}_boiler_ocr_{mod_name}.log"
        handler = logging.FileHandler(fname)
    else:
        handler = logging.StreamHandler()
    
    handler.setLevel(logging_level)
    handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(handler)
    
    return logger

def form_database_connection(user : str, pwd : str, host : str, db : str):
    database_url = f"mariadb+mariadbconnector://{user}:{pwd}@{host}/{db}"
    return database_url

def process_image(image, is_debug):
    gray_frame = cv2.cvtColor(image, cv2.COLOR_RGBA2GRAY)
    ret, image_to_test = cv2.threshold(gray_frame, 230, 200, cv2.THRESH_BINARY_INV)    
    if is_debug == True:
        cv2.imshow("Debug window", image_to_test)
        cv2.waitKey(0)

    return image_to_test

# Construir o endpoint para a fotografia
def form_source_endpoint(ip : str, port : str) -> str:
    endpoint = f"http://{ip}:{port}/video"
    return endpoint

def get_settings(file_name : str) -> dict:
    settings = None
    with open(file_name, 'r') as file:
        settings = json.load(file)
    return settings

def extract_text(frame, is_debug):
    image_to_parse = process_image(frame, is_debug) 
    text = pytesseract.image_to_string(image_to_parse, lang='lets', config="--oem 3 --psm 6 -c tessedit_char_whitelist=aA1234567890")
    return text.strip()

def connect(source):
    return cv2.VideoCapture(source, cv2.CAP_FFMPEG)

def handle_sigterm():
    raise KeyboardInterrupt

def cleanup(capture=None):
    if capture is not None:
        capture.release()
    cv2.destroyAllWindows()

def report_command(args):

    # logger
    main_logger = form_logger(args.debug, args.file_log, "report")
    
    # definições
    app_settings = get_settings(f"{args.settings}.json")

    database_url = form_database_connection(app_settings["app"]["database"]["user"],
                                            app_settings["app"]["database"]["password"],
                                            app_settings["app"]["database"]["host"],
                                            app_settings["app"]["database"]["database"]
                                            )

    # base de dados
    db_handler = MariaDBHandler(database_url, main_logger)

    records = db_handler.get_report_records_after()
    main_logger.info(f"Records fetched: {len(records)}")

    report_records = ReportProcessor(main_logger)
    records_to_persist = report_records.process_report_data(records)

    for record in records_to_persist:
        main_logger.info(f"Persisting report record with start time {record.start_time} and end time {record.end_time}")
        db_handler.insert_report_record(record)

    main_logger.info("Finished processing report data")

def run_command(args):
    signal.signal(signal.SIGTERM, handle_sigterm)

    # logger
    main_logger = form_logger(args.debug, args.file_log, "main")    

    # definições
    app_settings = get_settings(f"{args.settings}.json")
    pytesseract.pytesseract.tesseract_cmd = app_settings["ocr"]["tesseract-dir"]
    
    # Base de dados e video
    source = form_source_endpoint(app_settings["camera"]["connection"]["ip"], app_settings["camera"]["connection"]["port"])
    database_url = form_database_connection(app_settings["app"]["database"]["user"],
                                            app_settings["app"]["database"]["password"],
                                            app_settings["app"]["database"]["host"],
                                            app_settings["app"]["database"]["database"]
                                            )
    wait_time = 0 if "wait" not in app_settings["app"] else app_settings["app"]["wait"]

    feed_live = True
    main_logger.info("Video feed started. Analyzing frames.")
    connection_attempts = 0

    db_handler = None
    if args.dry_run == False:
        db_handler = MariaDBHandler(database_url, main_logger)
    
    main_logger.debug(f"Trying to connect to {source}")

    try:
        
        stop_recording = False
        previous_record = None
        # O smartphone fazia timeout se o objeto estivesse sempre instanciado

        while feed_live:
            capture = connect(source)
            main_logger.debug("Reading frame")

            if not capture.isOpened():
                capture.release()
                connection_attempts += 1
                if connection_attempts > CAMERA_CONNECTION_ATTEMPTS_LIMIT:
                    main_logger.critical("Couldn't open video feed. Giving up.")
                    return                    

                main_logger.warning(f"Couldn't open video feed. Retrying: {connection_attempts}")
                time.sleep(5)
                capture = connect(source)
                capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)                
                continue
            # Lê o frame
            ret, frame = capture.read()
            if not ret:
                main_logger.critical("Couldn't read frame.")
                return
            
            # Tesseract analisa a imagem e transforma numa string
            detected_text = extract_text(frame, args.debug)
            
            main_logger.debug(f"Detected Text: {detected_text}")
            result = None
            try:

                # Instanciar. Validações estão dentro do objeto                
                result = BoilerData(detected_text, main_logger, args.dry_run, db_handler)

                if previous_record is not None and (result.is_burning == previous_record.is_burning and 
                    result.temperature == previous_record.temperature and  
                    result.running_mode == previous_record.running_mode):
                    main_logger.debug("No significant change detected. Not persisting.")
                    continue 

                if result.is_burning == True and stop_recording == True:
                    main_logger.info("Boiler is on again. Resuming persistence.")
                    stop_recording = False

                if result.is_valid == True and stop_recording == False:
                    result.persist_run()
                    previous_record = result

                if result.is_burning == False and stop_recording == False:
                    main_logger.info("Boiler is off. Not persisting until turned on.")
                    stop_recording = True                    


            except Exception as e:
                main_logger.warning(f"Failed while forming the log. Retrying in the next cycle {e}. OCR is {detected_text}")
            
            cleanup(capture)
            main_logger.debug("Resources released. Waiting")
            time.sleep(wait_time)

    except KeyboardInterrupt:
        cleanup(capture)
        main_logger.info("Finished capture")
        return

def main():
    parser = argparse.ArgumentParser(
        description="Ferlux Boiler OCR System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s run --settings config --debug
  %(prog)s report --settings config --file-log
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=True)

    # Run subcommand
    run_parser = subparsers.add_parser('run', help='Start boiler data collection from camera')
    run_parser.add_argument("--debug", help="Enable debug logging", action="store_true")
    run_parser.add_argument("--file-log", help="Log to file instead of console", action="store_true")
    run_parser.add_argument("--dry-run", help="Run without persisting to database", action="store_true")
    run_parser.add_argument("--settings", help="Settings file name (without .json)", required=True)

    # Report subcommand
    report_parser = subparsers.add_parser('report', help='Generate analytics report from collected data')
    report_parser.add_argument("--debug", help="Enable debug logging", action="store_true")
    report_parser.add_argument("--file-log", help="Log to file instead of console", action="store_true")
    report_parser.add_argument("--dry-run", help="Run without persisting to database", action="store_true")
    report_parser.add_argument("--settings", help="Settings file name (without .json)", required=True)

    args = parser.parse_args()

    if args.command == 'run':
        run_command(args)
    elif args.command == 'report':
        report_command(args)

if __name__ == "__main__":
    main()