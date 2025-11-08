import pandas as pd
from utils.logger import logger

import pandas as pd
from utils.logger import logger

def generate_orders_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("🔹 Generating orders features...")

    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"], errors='coerce')
    df["promised_delivery_time"] = pd.to_datetime(df["promised_delivery_time"], errors='coerce')
    df["actual_delivery_time"] = pd.to_datetime(df["actual_delivery_time"], errors='coerce')

    # Temporal features
    df["day_of_week"] = df["order_date"].dt.day_name()
    df["month"] = df["order_date"].dt.month
    df["is_weekend"] = df["day_of_week"].isin(["Saturday", "Sunday"]).astype(int)

    # Delivery metrics
    df["delivery_delay_hrs"] = df["delivery_delay_hrs"].fillna(0)
    df["on_time_delivery"] = (df["delivery_delay_hrs"] <= 0).astype(int)

    logger.info(f"Orders features generated: {df.shape}")
    return df
