import logging
import os
from datetime import datetime

# Base directory for logs
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Create formatters
default_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_logger(name, level=logging.INFO):
    """Get a configured logger with the given name"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(default_formatter)
    
    # Create file handler
    log_file = os.path.join(LOG_DIR, f"{name}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(default_formatter)
    
    # Add handlers if not already added
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    
    return logger

def get_api_logger():
    """Get logger for API operations"""
    return get_logger("api")

def get_ingestion_logger(source_name):
    """Get logger for data ingestion operations"""
    return get_logger(f"ingestion.{source_name}")

def get_processing_logger():
    """Get logger for data processing operations"""
    return get_logger("processing")

def get_storage_logger():
    """Get logger for data storage operations"""
    return get_logger("storage")

def get_scheduler_logger():
    """Get logger for scheduler operations"""
    return get_logger("scheduler")