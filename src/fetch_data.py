# main.py
import sys
import os


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(CURRENT_DIR, "components")
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from utils.logger import logger
from config.db_config import get_mssql_connection
from config.db_schema import get_all_tables, get_table_columns
from tasks.extract_mssql import fetch_all_tables_parallel
from tasks.load_csv import load_all_csv
from tasks.geocode_enrichment import run_geocode_pipeline
from tasks.weather_enrichment import run_weather_pipeline    

# DB Connection Test

def test_db_connection():
    try:
        conn = get_mssql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT GETDATE();")
        row = cursor.fetchone()
        logger.info(f"Database connection successful. Current time from DB: {row[0]}")
        conn.close()
    except Exception as e:
        logger.error(f"Database connection failed: {e}")

# List Tables

def list_tables():
    try:
        tables = get_all_tables()
        logger.info(f"Tables defined in schemas: {tables}")
        return tables
    except Exception as e:
        logger.error(f"Failed to list tables: {e}")
        return []

# Show Columns

def show_table_columns(table_name):
    try:
        columns = get_table_columns(table_name)
        logger.info(f"Columns for table '{table_name}': {columns}")
    except Exception as e:
        logger.error(f"Failed to get columns for table '{table_name}': {e}")

# Parallel Fetch DB Tables

def parallel_fetch_all_tables():
    try:
        logger.info("Starting parallel fetch of all tables from database...")
        all_data = fetch_all_tables_parallel(max_workers=5)
        logger.info("Parallel fetch completed")

        for table, df in all_data.items():
            if df is not None:
                logger.info(f"{table}: {df.shape[0]} rows, {df.shape[1]} columns")
    except Exception as e:
        logger.error(f"Parallel fetch failed: {e}")

# Load CSV Files

def fetch_csv_data():
    try:
        data_folder = os.path.join(CURRENT_DIR, "data")
        logger.info(f"Loading all CSV files from '{data_folder}'...")
        all_csv_data = load_all_csv(data_folder)
        logger.info("Finished loading CSV files")

        for table, df in all_csv_data.items():
            logger.info(f"{table}: {df.shape[0]} rows, {df.shape[1]} columns")
    except Exception as e:
        logger.error(f"CSV loading failed: {e}")

# Main Pipeline

def main():
    logger.info("Starting Demand Forecasting Data Pipeline")

    # 1. Test DB connection
    test_db_connection()

    # 2. List tables
    tables = list_tables()

    # 3. Show columns for first table as example
    if tables:
        show_table_columns(tables[0])

    # 4. Parallel fetch DB tables
    parallel_fetch_all_tables()

    # 5. Fetch CSV data
    fetch_csv_data()

    # 6. Geocode enrichment
    try:
        logger.info("Starting geocode enrichment step...")
        run_geocode_pipeline()
        logger.info("Finished geocode enrichment step")
    except Exception as e:
        logger.error(f"Geocode enrichment step failed: {e}")

    # 7. Weather enrichment
    try:
        logger.info("Starting weather enrichment step...")
        run_weather_pipeline()
        logger.info(" Finished weather enrichment step")
    except Exception as e:
        logger.error(f" Weather enrichment step failed: {e}")

    logger.info(" Demand Forecasting Data Pipeline Completed")



if __name__ == "__main__":
    main()
