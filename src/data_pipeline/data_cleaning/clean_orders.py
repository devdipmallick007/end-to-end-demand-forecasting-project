import os
import logging
import pandas as pd

# Configure logging once for the whole pipeline
LOG_DIR = r"D:\demand_forecasting_system\logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "pipeline.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def clean_orders(df_orders: pd.DataFrame) -> pd.DataFrame:
    df = df_orders.copy()
    logs = {}

    logs["initial_rows"] = len(df)

    keep_cols = [
        "order_id", "customer_id", "order_date",
        "order_total", "payment_method", "store_id",
        "promised_delivery_time", "actual_delivery_time",
        "delivery_status"
    ]
    df = df[keep_cols]

    before = len(df)
    df.drop_duplicates(subset="order_id", keep="first", inplace=True)
    logs["duplicates_removed"] = before - len(df)

    logs["missing_before"] = df.isnull().sum().to_dict()

    df['payment_method'] = df['payment_method'].fillna('Unknown')
    df['delivery_status'] = df['delivery_status'].fillna('Pending')
    df['actual_delivery_time'] = df['actual_delivery_time'].fillna(df['promised_delivery_time'])
    # df['order_date'] = df['order_date'].dt.date
    datetime_cols = ["order_date", "promised_delivery_time", "actual_delivery_time"]
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    df['payment_method'] = df['payment_method'].str.title().replace({
        'Cod': 'COD',
        'Cc': 'Credit Card',
        'Dc': 'Debit Card'
    })

    df['delivery_status'] = df['delivery_status'].str.strip().str.title()

    Q1 = df['order_total'].quantile(0.25)
    Q3 = df['order_total'].quantile(0.75)
    IQR = Q3 - Q1
    upper_limit = Q3 + 1.5 * IQR
    lower_limit = Q1 - 1.5 * IQR

    before = len(df)
    df = df[(df['order_total'] >= lower_limit) & (df['order_total'] <= upper_limit)]
    logs["outliers_removed"] = before - len(df)

    df['delivery_delay_hrs'] = (
        (df['actual_delivery_time'] - df['promised_delivery_time'])
        .dt.total_seconds() / 3600
    )

    logs["final_rows"] = len(df)
    logs["rows_removed_total"] = logs["initial_rows"] - logs["final_rows"]

    # Write audit to pipeline.log
    logging.info("CLEANING REPORT: blinkit_orders")
    for k, v in logs.items():
        logging.info(f"{k}: {v}")

    return df
