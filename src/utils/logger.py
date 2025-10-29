# utils/logging.py
import logging
import os
from config.settings import LOG_PATH

# Ensure logs directory exists
os.makedirs(LOG_PATH, exist_ok=True)

# Configure logging
log_file = os.path.join(LOG_PATH, "pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Create logger instance
logger = logging.getLogger("data_pipeline")
def get_logger(name):
    return logging.getLogger(name)  
