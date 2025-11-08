# # utils/logging.py
# import logging
# import os
# from config.settings import LOG_PATH
# import sys
# sys.stdout.reconfigure(encoding='utf-8')
# # Ensure logs directory exists
# os.makedirs(LOG_PATH, exist_ok=True)

# # Configure logging
# log_file = os.path.join(LOG_PATH, "pipeline.log")
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
#     handlers=[
#         logging.FileHandler(log_file),
#         logging.StreamHandler()
#     ]
# )

# # Create logger instance
# logger = logging.getLogger("data_pipeline")
# def get_logger(name):
#     return logging.getLogger(name)  


# ==============================================================
#                 LOGGER CONFIGURATION (UTF-8 SAFE)
# ==============================================================

import logging
import os
import sys
from config.settings import LOG_PATH

# --- 1. Force UTF-8 output for Windows consoles ---
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass  # safe fallback if not supported

# --- 2. Ensure logs directory exists ---
os.makedirs(LOG_PATH, exist_ok=True)

# --- 3. Define log file path ---
log_file = os.path.join(LOG_PATH, "pipeline.log")

# --- 4. Create file + console handlers ---
file_handler = logging.FileHandler(log_file, encoding='utf-8')
console_handler = logging.StreamHandler(sys.stdout)

# --- 5. Define a consistent formatter ---
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# --- 6. Configure root logger ---
logger = logging.getLogger("data_pipeline")
logger.setLevel(logging.INFO)

# Avoid duplicate handlers if re-imported
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# --- 7. Utility function for module-level loggers ---
def get_logger(name: str):
    """Get a named logger inheriting from the main configuration."""
    return logging.getLogger(f"data_pipeline.{name}")
