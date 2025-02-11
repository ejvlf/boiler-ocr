from sqlalchemy import create_engine, MetaData, Table, Column, Integer, SmallInteger , String, Boolean, insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

import logging

class MariaDBHandler:
    def __init__(self, db_url: str, log : logging.Logger):
        self.log = log
        self.engine = create_engine(db_url)
        self.metadata = MetaData()
        self.records = Table(
            "records", self.metadata,
            Column("SystemTimestamp", Integer, primary_key=True),
            Column("Temperature", SmallInteger, nullable=False),
            Column("MarkedTime", String(20), nullable=True),
            Column("RunningMode", String(1), nullable=True),
            Column("IsBurning", Boolean, nullable=False)
        )

    def generate_insert_query(self, pk : int, temp : int, stamped_time : str, mode : str, is_burning) -> str:
        stmt = insert(self.records).values(SystemTimestamp=pk,
                                        Temperature=temp,
                                        MarkedTime=stamped_time,
                                        RunningMode=mode,
                                        IsBurning=is_burning)
        return stmt.compile(self.engine, compile_kwargs={"literal_binds": True}).string

    def insert_user(self, name: str, email: str):
        """Insert a user into the database with error handling."""
        query_str = self.generate_insert_query(name, email)
        self.log.debug("Generated SQL:", query_str)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query_str)
                conn.commit()
                return result.inserted_primary_key
        except IntegrityError as e:
            self.log.warning(f"IntegrityError: {e.orig}")  # Likely a NULL or unique constraint violation
            return None
        except SQLAlchemyError as e:
            self.log.critical(f"Database Error: {e}")  # General SQLAlchemy error
            return None
        except Exception as e:
            self.log.critical(f"Unexpected Error: {e}")  # Catch-all for unexpected errors
            return None

# Example Usage:
if __name__ == "__main__":
    db_url = "mysql+pymysql://username:password@host:3306/database"
    db_handler = MariaDBHandler(db_url)

    # Generate SQL query without executing
    sql_query = db_handler.generate_insert_query("John Doe", "john@example.com")
    print("Generated SQL Query:", sql_query)

    # Insert user into the database
    new_user_id = db_handler.insert_user("John Doe", "john@example.com")
    if new_user_id:
        print("Inserted user ID:", new_user_id)
    else:
        print("Insert failed due to constraint violation or error.")
