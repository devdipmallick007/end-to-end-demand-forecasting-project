import os
import logging
import pandas as pd

# Log Directory Setup
LOG_DIR = r"D:\demand_forecasting_system\logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "pipeline.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def clean_order_items(df_order_items: pd.DataFrame) -> pd.DataFrame:
    df = df_order_items.copy()
    logs = {}

    logs["initial_rows"] = len(df)

    # Keep only relevant columns
    usable_cols = ['order_id', 'product_id', 'quantity', 'unit_price']
    df = df[usable_cols].copy()

    # Remove duplicates
    before = len(df)
    df.drop_duplicates(subset=['order_id', 'product_id'], inplace=True)
    logs["duplicates_removed"] = before - len(df)

    # Track missing before cleaning
    logs["missing_before"] = df.isnull().sum().to_dict()

    # Fill missing numeric values
    df['quantity'] = df['quantity'].fillna(0)
    df['unit_price'] = df['unit_price'].fillna(0)

    # Remove rows missing important IDs
    before = len(df)
    df.dropna(subset=['order_id', 'product_id'], inplace=True)
    logs["rows_removed_missing_id"] = before - len(df)

    # Fix dtypes
    df['order_id'] = df['order_id'].astype(int)
    df['product_id'] = df['product_id'].astype(int)
    df['quantity'] = df['quantity'].astype(int)
    df['unit_price'] = df['unit_price'].astype(float)

    # Calculate total price
    df['total_price'] = df['quantity'] * df['unit_price']

    logs["final_rows"] = len(df)
    logs["rows_removed_total"] = logs["initial_rows"] - logs["final_rows"]

    # Write logs to file
    logging.info("CLEANING REPORT: blinkit_order_items")
    for k, v in logs.items():
        logging.info(f"{k}: {v}")

    return df
