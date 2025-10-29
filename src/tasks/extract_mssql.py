# src/tasks/extract_mssql.py

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.db_config import get_mssql_connection
from config.db_schema import get_all_tables
from utils.logger import logger


def fetch_table_data(table_name):
    """
    Fetch table data directly from DB.
    """
    try:
        conn = get_mssql_connection()
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, conn)
        conn.close()
        logger.info(f"Fetched {len(df)} rows from table '{table_name}' (DB)")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch table '{table_name}': {e}")
        return None


def fetch_all_tables_parallel(max_workers=5):
    """
    Fetch all tables in parallel from DB.
    Returns a dictionary {table_name: DataFrame}.
    """
    all_tables = get_all_tables()
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_table_data, table): table for table in all_tables}
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                df = future.result()
                if df is not None:
                    results[table_name] = df
                else:
                    results[table_name] = pd.DataFrame()
                    logger.warning(f"No data returned for table '{table_name}'")
            except Exception as e:
                logger.error(f"Error fetching table '{table_name}': {e}")
                results[table_name] = pd.DataFrame()

    return results
