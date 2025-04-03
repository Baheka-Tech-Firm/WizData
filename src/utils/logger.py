import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from src.utils.config import LOG_DIR

def setup_logger(name, log_file=None, level=logging.INFO):
    """Set up a logger with file and console handlers"""
    
    # Create a custom logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    # Add console handler to logger
    logger.addHandler(console_handler)
    
    # If log file is provided, create file handler
    if log_file:
        # Create logs directory if it doesn't exist
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Create file handler
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        
        # Add file handler to logger
        logger.addHandler(file_handler)
    
    return logger

# Create loggers for different components
def get_ingestion_logger(source_name):
    log_file = os.path.join(LOG_DIR, f"ingestion_{source_name}.log")
    return setup_logger(f"wizdata.ingestion.{source_name}", log_file)

def get_processing_logger():
    log_file = os.path.join(LOG_DIR, "processing.log")
    return setup_logger("wizdata.processing", log_file)

def get_storage_logger(db_type):
    log_file = os.path.join(LOG_DIR, f"storage_{db_type}.log")
    return setup_logger(f"wizdata.storage.{db_type}", log_file)

def get_api_logger():
    log_file = os.path.join(LOG_DIR, "api.log")
    return setup_logger("wizdata.api", log_file, level=logging.DEBUG)

def get_scheduler_logger():
    log_file = os.path.join(LOG_DIR, "scheduler.log")
    return setup_logger("wizdata.scheduler", log_file)
