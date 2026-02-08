from sqlalchemy import DateTime, Numeric, Time, create_engine, MetaData, Table, Column, Integer, SmallInteger , String, Boolean, func, insert, select, null
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

import logging
from datetime import datetime

from objects.analytics import ReportData

class MariaDBHandler:
    def __init__(self, db_url: str, log : logging.Logger):
        self.log = log
        self.engine = create_engine(db_url)
        self.log.info("Connecting to database...")
        self.connection = self.engine.connect()
        self.log.info(f"Connected to database {self.engine.url}")

        self.metadata = MetaData()
        self.records = Table(
            "records", self.metadata,
            Column("SystemTimestamp", Integer, primary_key=True),
            Column("Temperature", SmallInteger, nullable=False),
            Column("MarkedTime", String(20), nullable=True),
            Column("RunningMode", String(1), nullable=True),
            Column("IsBurning", Boolean, nullable=False)
        )
        self.report = Table(
            "report", self.metadata,
            Column("ID", Integer, primary_key=True),
            Column("StartTime", DateTime, nullable=False),
            Column("EndTime", DateTime, nullable=False),
            Column("AvgTemperature", Numeric(3,1), nullable=False),
            Column("Mode1", Time, nullable=False),
            Column("Mode2", Time, nullable=False),
            Column("Mode3", Time, nullable=False),
            Column("Mode4", Time, nullable=False),
            Column("Mode5", Time, nullable=False),
            Column("ModeA", Time, nullable=False),
            Column("TotalDuration", Time, nullable=False),
            Column("HasStandby", Boolean, nullable=False)
        )
        self.consumption = Table(
            "consumption", self.metadata,
            Column("ID", Integer, primary_key=True),
            Column("ReportID", Integer, nullable=False),
            Column("Quantity", Numeric(2,1), nullable=False),
            Column("MaxRoomTemperature", Numeric(3,1), nullable=False),
            Column("MaxBoilerTemperature", Numeric(3,1), nullable=False)
        )        
    def get_reporting_last_end_time(self) -> datetime:
        try:
            stmt = select(func.max(self.report.c.EndTime))

            result = self.connection.execute(stmt)
            latest_end_time = result.scalar()
            
            if latest_end_time is None:
                self.log.info("No records found in report table, returning default date")
                return datetime(2026, 2, 3, 0, 0, 0)

            return latest_end_time
            
        except SQLAlchemyError as e:
            self.log.error(f"Error fetching latest EndTime: {e}")
            return datetime(2026, 2, 3)
    def get_partial_reports(self) -> list[tuple]:
        try:
            stmt = (
                select(self.report.c.ID, self.report.c.StartTime, self.report.c.EndTime)
                .outerjoin(self.consumption, self.report.c.ID == self.consumption.c.ReportID)
                .where(self.consumption.c.ReportID.is_(None))
            )
            result = self.connection.execute(stmt)
            all_results = result.fetchall()
            return all_results
        except SQLAlchemyError as e:
            self.log.error(f"Error fetching latest missing results: {e}")
            return None
    def insert_consumption_record(self, data : dict):
        stmt = insert(self.consumption).values(
                                        ID=data["id"],            
                                        ReportID=data["report_id"],
                                        Quantity=data["quantity"],
                                        MaxRoomTemperature=data["max_room_temperature"],
                                        MaxBoilerTemperature=data["max_boiler_temperature"]
                                        )
        stmt.compile(self.engine, compile_kwargs={"literal_binds": True})
        try:
            
            result = self.connection.execute(stmt)
            self.connection.commit()
            self.log.info(f"Inserted consumption record with ID {result.inserted_primary_key[0]}")
                
            return result.inserted_primary_key[0]
        except IntegrityError as e:
            self.log.error(f"Integrity Error: {e.orig}")  # NULL
            return None
        except SQLAlchemyError as e:
            self.log.critical(f"Command Error: {e}") 
            return None
        except Exception as e:
            self.log.critical(f"Unexpected Error: {e}")
            return None
    def get_report_records_after(self):
        try:
            timestamp = self.get_reporting_last_end_time()
            timestamp_int = int(timestamp.timestamp())
            stmt = select(self.records).where(self.records.c.SystemTimestamp > timestamp_int
                                              ).order_by(self.records.c.SystemTimestamp.asc())

            result = self.connection.execute(stmt)

            return result.fetchall()
        except TypeError as e:
            self.log.error(f"Type error on {timestamp}: {e}")
            return []
        except SQLAlchemyError as e:
            self.log.error(f"Error fetching records after {timestamp}: {e}")
            return []
    def insert_report_record(self, report_object : ReportData):
        stmt = insert(self.report).values(StartTime=report_object.start_time,
                                        EndTime=report_object.end_time,
                                        AvgTemperature=report_object.avg_temperature,
                                        Mode1=report_object.operation_time["mode1"],
                                        Mode2=report_object.operation_time["mode2"],
                                        Mode3=report_object.operation_time["mode3"],
                                        Mode4=report_object.operation_time["mode4"],
                                        Mode5=report_object.operation_time["mode5"],
                                        ModeA=report_object.operation_time["modeA"],
                                        TotalDuration=report_object.total_duration,
                                        HasStandby=report_object.has_standby
                                        )
        stmt.compile(self.engine, compile_kwargs={"literal_binds": True})
        try:
            
            result = self.connection.execute(stmt)
            self.connection.commit()
            self.log.info(f"Inserted report record with ID {result.inserted_primary_key[0]}")
                
            return result.inserted_primary_key[0]
        except IntegrityError as e:
            self.log.error(f"Integrity Error: {e.orig}")  # NULL
            return None
        except SQLAlchemyError as e:
            self.log.critical(f"Command Error: {e}") 
            return None
        except Exception as e:
            self.log.critical(f"Unexpected Error: {e}")
            return None

    def insert_record(self, record_object):
        pk = int(datetime.now().timestamp())
        stmt = insert(self.records).values(SystemTimestamp=pk,
                                        Temperature=record_object.temperature,
                                        MarkedTime=record_object.marked_time.strftime("%Y-%m-%dT%H:%MZ"),
                                        RunningMode=record_object.running_mode,
                                        IsBurning=record_object.is_burning)
        stmt.compile(self.engine, compile_kwargs={"literal_binds": True})

        try:
            
            result = self.connection.execute(stmt)
            self.connection.commit()
                
            if result.inserted_primary_key[0] is None:
                return pk
                
            return result.inserted_primary_key[0]
        except IntegrityError as e:
            self.log.error(f"Integrity Error: {e.orig}")  #NULL
            return None
        except SQLAlchemyError as e:
            self.log.critical(f"Command Error: {e}")  
            return None
        except Exception as e:
            self.log.critical(f"Unexpected Error: {e}")
            return None
    def __del__(self):
        self.connection.close()
        self.log.info("DB Connection closed")