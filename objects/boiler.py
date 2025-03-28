from datetime import datetime

class BoilerData:
    def __init__(self, raw_data, logger, is_dry_run, db_handler):
        self.log = logger
        self.is_burning = self._form_is_burning(raw_data)
        self.temperature = self._form_temperature(raw_data)
        self.marked_time = self._form_marked_time(raw_data)
        self.running_mode = self._form_running_mode(raw_data)
        self.dry_run = is_dry_run
        self.db_handler = db_handler
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
            running_mode = mode_from_string[0:1].strip()
        return running_mode
    def _form_temperature(self, raw_data):
        try:
            temperature_to_return = 0
            temperature_as_string = raw_data.splitlines()
            if self.is_burning:
                temperature_to_return = int(temperature_as_string[1][-2:].strip())
            else:
                temperature_to_return = int(temperature_as_string[1].strip())
        except ValueError as e:
            self.log.error(f"Couldn't get proper temperature from ocr {raw_data}. Setting to 0.")
        return temperature_to_return
        
    def persist_run(self):

        if self.dry_run is False and self.db_handler is None:
            self.log.error("Couldn't generate db string. Consider changing to dry run")
            self.dry_run = True
        
        if self.dry_run:
            burning = "No" if self.is_burning == False else "Yes"
            self.log.info(f"Current status: Marked time - {self.marked_time}|Temperature - {self.temperature}|Running mode - {self.running_mode}|Burning - {burning}")
            return

        timestamp_inserted = self.db_handler.insert_record(self)
        if timestamp_inserted:
            self.log.info(f"Inserted timestamp: {timestamp_inserted}")
        else:
            self.log.error(f"Insert failed due to constraint violation or error.")
