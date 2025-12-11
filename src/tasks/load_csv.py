# src/tasks/load_csv.py

import os
import pandas as pd
from utils.logger import logger

def load_all_csv(data_folder="D:\\demand_forecasting_system\\data\\raw_data"):
    """
    Load all CSV files from the given folder into memory.
    Returns a dictionary: {filename_without_ext: DataFrame}
    """
    if not os.path.exists(data_folder):
        logger.error(f"Data folder '{data_folder}' does not exist")
        return {}

    csv_files = [f for f in os.listdir(data_folder) if f.lower().endswith(".csv")]
    if not csv_files:
        logger.warning(f"No CSV files found in '{data_folder}'")
        return {}

    all_data = {}
    for csv_file in csv_files:
        file_path = os.path.join(data_folder, csv_file)
        try:
            df = pd.read_csv(file_path)
            table_name = os.path.splitext(csv_file)[0]  # remove .csv extension
            all_data[table_name] = df
            logger.info(f"Loaded CSV '{csv_file}' with {df.shape[0]} rows, {df.shape[1]} columns")
        except Exception as e:
            logger.error(f"Failed to load CSV '{csv_file}': {e}")

    return all_data


if __name__ == "__main__":
    logger.info("Loading all CSVs from data folder...")
    data = load_all_csv()
    logger.info("Finished loading all CSVs")
