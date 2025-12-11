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


def clean_customers(df_customers: pd.DataFrame) -> pd.DataFrame:
    df = df_customers.copy()
    logs = {}

    logs["initial_rows"] = len(df)

    # Select usable columns
    keep_cols = [
        "customer_id", "area", "pincode",
        "registration_date", "customer_segment",
        "total_orders", "avg_order_value"
    ]
    df = df[keep_cols].copy()

    # Missing values before cleaning
    logs["missing_before"] = df.isnull().sum().to_dict()

    # Fill missing categorical fields
    df["area"] = df["area"].fillna("Unknown")
    df["customer_segment"] = df["customer_segment"].fillna("Regular")

    # Fill missing numeric fields
    df["total_orders"] = df["total_orders"].fillna(df["total_orders"].median())
    df["avg_order_value"] = df["avg_order_value"].fillna(df["avg_order_value"].median())

    # Convert dates
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")

    # Fix data types
    df = df.astype({
        "customer_id": "int64",
        "pincode": "Int64",
        "total_orders": "int64",
        "avg_order_value": "float64"
    })

    before = len(df)
    df.drop_duplicates(subset="customer_id", inplace=True)
    logs["duplicates_removed"] = before - len(df)

    logs["final_rows"] = len(df)
    logs["rows_removed_total"] = logs["initial_rows"] - logs["final_rows"]

    # Log everything cleanly
    logging.info("CLEANING REPORT: blinkit_customers")
    for k, v in logs.items():
        logging.info(f"{k}: {v}")

    return df
