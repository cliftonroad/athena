# app/models/control.py
from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class ControlTable(Base):
    __tablename__ = 'file_processing_control'
    
    id = Column(Integer, primary_key=True)
    process_id = Column(String(50), nullable=False)
    file_name = Column(String(255), nullable=False)
    file_path = Column(String(1000), nullable=False)
    target_table = Column(String(100), nullable=False)
    status = Column(String(50), nullable=False)
    total_rows = Column(Integer)
    loaded_rows = Column(Integer)
    error_message = Column(Text)
    current_batch = Column(Integer)
    file_location = Column(String(1000))  # Current location of the file
    created_date = Column(DateTime, default=datetime.utcnow)
    created_by = Column(String(100))
    modified_date = Column(DateTime, onupdate=datetime.utcnow)
    modified_by = Column(String(100))