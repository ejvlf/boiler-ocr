from sqlalchemy import create_engine, MetaData, Table, Column, Integer, SmallInteger , String, Boolean, insert, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

import logging
from datetime import datetime

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

    def _generate_insert_query(self, pk : int, temp : int, stamped_time : datetime, mode : str, is_burning) -> str:
        stmt = insert(self.records).values(SystemTimestamp=pk,
                                        Temperature=temp,
                                        MarkedTime=stamped_time.strftime("%Y-%m-%dT%H:%MZ"),
                                        RunningMode=mode,
                                        IsBurning=is_burning)
        return stmt.compile(self.engine, compile_kwargs={"literal_binds": True})

    def insert_record(self, record_object):
        pk = int(datetime.now().timestamp())
        query_str = self._generate_insert_query(pk, 
                                            record_object.temperature,
                                            record_object.marked_time,
                                            record_object.running_mode,
                                            record_object.is_burning
                                            )
        self.log.debug("Generated SQL:", query_str.string)

        try:
            
            result = self.connection.execute(query_str)
            self.connection.commit()
                
            if result.inserted_primary_key[0] is None:
                return pk
                
            return result.inserted_primary_key[0]
        except IntegrityError as e:
            self.log.error(f"Integrity Error: {e.orig}")  # Likely a NULL violation
            return None
        except SQLAlchemyError as e:
            self.log.critical(f"Command Error: {e}")  # General SQLAlchemy error
            return None
        except Exception as e:
            self.log.critical(f"Unexpected Error: {e}")  # Catch-all for unexpected errors
            return None
    def __del__(self):
        self.connection.close()
        self.log.info("DB Connection closed")