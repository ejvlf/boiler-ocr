from sqlalchemy import Column, Integer, DateTime, String, Boolean
from sqlalchemy.orm import Session, relationship, declarative_base
from sqlalchemy.exc import SQLAlchemyError

Base = declarative_base()

class Record(Base):
    __tablename__ = "records"
    CreatedAt = Column(DateTime, primary_key=True)
    RecordedTimestamp = Column(DateTime, nullable=False)
    Temperature = Column(Integer, nullable=False)
    Mode = Column(String(1), nullable=False)
    IsBurning = Column(Boolean, nullable=False)

class Database:
    def __init__(self, model, log):
        self.model = model
        self.log = log

    def create(self, db: Session, **kwargs):
        try:
            obj = self.model(**kwargs)
            db.add(obj)
            db.commit()
            db.refresh(obj)
            return obj
        except SQLAlchemyError as e:
            db.rollback()
            self.log.warning(f"Error creating record: {e}")
            return None

    def read(self, db: Session, obj_id: int):
        return db.query(self.model).filter_by(id=obj_id).first()

    def update(self, db: Session, obj_id: int, **kwargs):
        obj = self.read(db, obj_id)
        if obj:
            for key, value in kwargs.items():
                setattr(obj, key, value)
            try:
                db.commit()
                db.refresh(obj)
                return obj
            except SQLAlchemyError as e:
                db.rollback()
                self.log.warning(f"Error updating record: {e}")
                return None
        return None

    def delete(self, db: Session, obj_id: int):
        obj = self.read(db, obj_id)
        if obj:
            try:
                db.delete(obj)
                db.commit()
                return True
            except SQLAlchemyError as e:
                db.rollback()
                self.log.warning(f"Error deleting record: {e}")
                return False
        return False
