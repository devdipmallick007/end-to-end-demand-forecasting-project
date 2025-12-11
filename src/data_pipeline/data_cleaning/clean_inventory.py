import os
import logging
import pandas as pd

# Logs directory setup
LOG_DIR = r"D:\demand_forecasting_system\logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "pipeline.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def clean_inventory_data(df_inventory: pd.DataFrame) -> pd.DataFrame:
    df = df_inventory.copy()
    logs = {}

    logs["initial_rows"] = len(df)

    #  Keep only relevant columns
    usable_cols = [
        "product_id", "date", "stock_received", "damaged_stock"
    ]
    df = df[usable_cols].copy()

    #  Remove duplicates based on product_id + date
    before = len(df)
    df.drop_duplicates(subset=["product_id", "date"], inplace=True)
    logs["duplicates_removed"] = before - len(df)

    #  Missing value report before cleaning
    logs["missing_before"] = df.isnull().sum().to_dict()

    #  Fill missing numeric values with 0
    num_cols = ["stock_received", "damaged_stock"]
    for col in num_cols:
        df[col] = df[col].fillna(0)

    #  Remove rows where product_id OR date is missing
    before = len(df)
    df.dropna(subset=["product_id", "date"], inplace=True)
    logs["rows_removed_missing_pk"] = before - len(df)

    #  Convert datatypes
    df["product_id"] = df["product_id"].astype(int)
    df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y", errors="coerce")

    #  Treat outliers using 99th percentile cap
    for col in num_cols:
        upper_limit = df[col].quantile(0.99)
        df.loc[df[col] > upper_limit, col] = upper_limit

    #  Missing report after cleaning
    logs["missing_after"] = df.isnull().sum().to_dict()

    logs["final_rows"] = len(df)
    logs["rows_removed_total"] = logs["initial_rows"] - logs["final_rows"]

    #  Log cleaning performance
    logging.info(" CLEANING REPORT: Inventory Data")
    for k, v in logs.items():
        logging.info(f"{k}: {v}")

    return df
