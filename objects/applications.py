import logging
import json
from datetime import datetime

def get_settings(file_name : str) -> dict:
    settings = None
    with open(file_name, 'r') as file:
        settings = json.load(file)
    return settings

def form_database_connection(user : str, pwd : str, host : str, db : str):
    database_url = f"mariadb+mariadbconnector://{user}:{pwd}@{host}/{db}"
    return database_url

def form_logger(is_debug: bool, is_file_log: bool, mod_name: str) -> logging.Logger:

    logging_level = logging.DEBUG if is_debug else logging.INFO
    
    # Create logger
    logger = logging.getLogger(f"Boiler OCR - {mod_name}")
    logger.setLevel(logging_level)
    
    logger.handlers.clear()    
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    
    if is_file_log:
        fname = f"{datetime.now().date().strftime('%Y-%m-%d')}_boiler_ocr_{mod_name}.log"
        handler = logging.FileHandler(fname)
    else:
        handler = logging.StreamHandler()
    
    handler.setLevel(logging_level)
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    return logger