import os
import logging
import pandas as pd
import numpy as np

# Logs directory setup
LOG_DIR = r"D:\demand_forecasting_system\logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "pipeline.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def clean_products_data(df_products: pd.DataFrame) -> pd.DataFrame:
    df = df_products.copy()
    logs = {}

    logs["initial_rows"] = len(df)

    # Keep only relevant columns
    usable_cols = [
        'product_id', 'product_name', 'category', 'brand',
        'price', 'mrp', 'margin_percentage',
        'shelf_life_days', 'min_stock_level', 'max_stock_level'
    ]
    df = df[usable_cols].copy()

    # Remove duplicates
    before = len(df)
    df.drop_duplicates(subset='product_id', inplace=True)
    logs["duplicates_removed"] = before - len(df)

    # Missing value report before cleaning
    logs["missing_before"] = df.isnull().sum().to_dict()

    # Categorical fix
    cat_cols = ['product_name', 'category', 'brand']
    df[cat_cols] = df[cat_cols].fillna('Unknown')

    # Numeric fix
    num_cols = [
        'price', 'mrp', 'margin_percentage',
        'shelf_life_days', 'min_stock_level', 'max_stock_level'
    ]
    df[num_cols] = df[num_cols].fillna(0)

    # Correct dtypes
    df['product_id'] = df['product_id'].astype(int)
    for col in ['price', 'mrp', 'margin_percentage']:
        df[col] = df[col].astype(float)
    for col in ['shelf_life_days', 'min_stock_level', 'max_stock_level']:
        df[col] = df[col].astype(int)

    # Profit per product
    df['profit_per_unit'] = df['mrp'] - df['price']

    # Discount %
    df['discount_percentage'] = (
        (df['mrp'] - df['price']) / df['mrp']
    ).replace([np.inf, -np.inf, np.nan], 0)

    logs["final_rows"] = len(df)
    logs["rows_removed_total"] = logs["initial_rows"] - logs["final_rows"]

    # Log results to file
    logging.info("CLEANING REPORT: blinkit_products")
    for k, v in logs.items():
        logging.info(f"{k}: {v}")

    return df
