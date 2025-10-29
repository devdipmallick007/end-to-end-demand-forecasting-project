# config/db_config.py
import pyodbc
from config.settings import DB_DRIVER, DB_SERVER, DB_NAME

def get_mssql_connection():
    """Return a connection to the MSSQL database."""
    conn_str = (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"Trusted_Connection=yes;"
    )
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        from utils.logger import logger
        logger.error(f"Database connection failed: {e}")
        raise
