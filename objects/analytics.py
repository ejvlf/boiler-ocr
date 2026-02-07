from datetime import datetime
import logging
import math
from statistics import mean
import time

from datetime import time, datetime, timedelta

class ReportData:
    def __init__(self, logger : logging.Logger):
        self.log = logger
        self._start_time = None
        self._end_time = None
        self._avg_temperature = []
        self.max_room_temperature = 22
        self._mode_1 = timedelta(0)
        self._mode_2 = timedelta(0)
        self._mode_3 = timedelta(0)
        self._mode_4 = timedelta(0)
        self._mode_5 = timedelta(0)
        self._mode_A = timedelta(0)
        self._total_duration = timedelta(0)
        self.has_standby = False

    def parse_timedelta_to_time(self, td: timedelta) -> time:
        total_seconds = int(td.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return time(hour=hours, minute=minutes, second=seconds)

    @property
    def start_time(self):
        return self._start_time
    @start_time.setter
    def start_time(self, unix_timestamp: int):

        if self._start_time is None:
            self._start_time = datetime.fromtimestamp(unix_timestamp)

    @property
    def end_time(self):
        return self._end_time

    @property
    def operation_time(self):

        return {"mode1": self.parse_timedelta_to_time(self._mode_1),
                "mode2": self.parse_timedelta_to_time(self._mode_2),
                "mode3": self.parse_timedelta_to_time(self._mode_3),
                "mode4": self.parse_timedelta_to_time(self._mode_4),
                "mode5": self.parse_timedelta_to_time(self._mode_5),
                "modeA": self.parse_timedelta_to_time(self._mode_A)
                }
    @operation_time.setter
    def operation_time(self, data : tuple[str, int]):
        mode = str(data[0])
        unix_timestamp = int(data[1])

        # Lixado - Pode já ter ocorrido um incremento nos modos. Assim faz se a diferença de tempo
        # entre eles todos. A ordem garante precisão.  
        modes_diff = (self._mode_1 + self._mode_2 + self._mode_3 + self._mode_4 + self._mode_5 + self._mode_A)

        if mode == "1":
            self._mode_1 += timedelta(seconds=unix_timestamp - self.start_time.timestamp() - modes_diff.total_seconds())
        elif mode == "2":
            self._mode_2 += timedelta(seconds=unix_timestamp - self.start_time.timestamp() - modes_diff.total_seconds())
        elif mode == "3":
            self._mode_3 += timedelta(seconds=unix_timestamp - self.start_time.timestamp() - modes_diff.total_seconds())
        elif mode == "4":
            self._mode_4 += timedelta(seconds=unix_timestamp - self.start_time.timestamp() - modes_diff.total_seconds())
        elif mode == "5":
            self._mode_5 += timedelta(seconds=unix_timestamp - self.start_time.timestamp() - modes_diff.total_seconds())
        elif mode == "A":
            self._mode_A += timedelta(seconds=unix_timestamp - self.start_time.timestamp() - modes_diff.total_seconds())

    @end_time.setter
    def end_time(self, unix_timestamp: int):

        if self._end_time is None:
            self._end_time = datetime.fromtimestamp(unix_timestamp)

    @property
    def avg_temperature(self):
        return round(mean(self._avg_temperature),1)
    
    @avg_temperature.setter
    def avg_temperature(self, temperature: int):
        self._avg_temperature.append(temperature)

    @property
    def total_duration(self):
        time_diff = self._end_time - self._start_time
        return self.parse_timedelta_to_time(time_diff)

class ReportProcessor:
    def __init__(self, logger : logging.Logger):
        self.log = logger
    
    def process_report_data(self, raw_data : list[tuple]) -> list[ReportData]:
        report_data_list = []
        current_report = None

        for row in raw_data:
            if row[4] == True and current_report is None:
                current_report = ReportData(self.log)
                current_report.start_time = row[0]
            
            if row[4] == True and current_report is not None:
                current_report.avg_temperature = row[1]
                current_report.operation_time = (row[3], row[0])
            
            elif row[4] == False and current_report is not None:
                current_report.end_time = row[0]
                report_data_list.append(current_report)
                current_report = None
        
        return report_data_list

