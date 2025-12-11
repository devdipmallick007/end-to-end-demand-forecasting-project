import os
import sys
import pandas as pd
from datetime import datetime


PROJECT_ROOT = r"D:\demand_forecasting_system"
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)


from utils.logger import logger
from tasks.extract_mssql import fetch_table_data
from tasks.load_csv import load_all_csv


from data_pipeline.data_cleaning.clean_orders import clean_orders
from data_pipeline.data_cleaning.clean_orders_items import clean_order_items
from data_pipeline.data_cleaning.clean_customer_data import clean_customers
from data_pipeline.data_cleaning.clean_marketing import clean_marketing_data
from data_pipeline.data_cleaning.clean_products_data import clean_products_data
from data_pipeline.data_cleaning.clean_weather import clean_weather_data
from data_pipeline.data_cleaning.clean_inventory import clean_inventory_data


def save_cleaned_data(df: pd.DataFrame, filename: str):
    save_path = os.path.join(PROJECT_ROOT, "data", "processed", filename)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False)
    print(f" Saved cleaned file: {filename}")
    return save_path


# Load all CSV files once
csv_files = load_all_csv()


def run_cleaning_pipeline():
    print("\n Starting Full Data Cleaning Pipeline...\n")
    start_time = datetime.now()

    tasks = [
        ("blinkit_orders", clean_orders, "blinkit_orders_clean.csv"),
        ("blinkit_order_items", clean_order_items, "blinkit_order_items_clean.csv"),
        ("blinkit_customers", clean_customers, "blinkit_customers_clean.csv"),
        ("blinkit_marketing_performance", clean_marketing_data, "blinkit_marketing_clean.csv"),
        ("blinkit_products", clean_products_data, "blinkit_products_clean.csv"),
        ("blinkit_weather_data", clean_weather_data, "blinkit_weather_clean.csv"),
        ("blinkit_inventory", clean_inventory_data, "blinkit_inventory_clean.csv"),
    ]

    for table_name, cleaner_func, output_file in tasks:
        print(f"Fetching & Cleaning: {table_name} ...")
        df = None

        # Try database
        try:
            df = fetch_table_data(table_name)
        except Exception as e:
            logger.error(f"DB fetch failed for {table_name}: {e}")

        # CSV fallback if DB fails or empty
        if df is None or df.empty:
            csv_name = table_name.replace("_performance", "")  # Marketing mapping fix
            df = csv_files.get(csv_name)

            if df is not None:
                print(f" Fallback: Loaded {csv_name}.csv from local data folder")
            else:
                print(f" Skipping {table_name} — No DB or CSV data found")
                logger.warning(f"{table_name}: No DB or CSV data available. Skipped.")
                print("-" * 60)
                continue

        # Clean + Save output
        try:
            df_clean = cleaner_func(df)
            save_cleaned_data(df_clean, output_file)
            print(f" Completed: {table_name}")
        except Exception as e:
            logger.error(f"Cleaning failed for {table_name}: {e}")
            print(f" ERROR cleaning {table_name}")

        print("-" * 60)

    duration = (datetime.now() - start_time).total_seconds()
    print(f"\n Pipeline Finished in {duration:.2f} sec")
    print(" Full logs saved to logs/pipeline.log\n")


if __name__ == "__main__":
    run_cleaning_pipeline()
