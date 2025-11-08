import pandas as pd
from utils.logger import logger

def generate_customer_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("🔹 Generating customer features...")

    df = df.copy()
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors='coerce')
    df["customer_tenure_days"] = (pd.Timestamp.now() - df["registration_date"]).dt.days
    df["order_frequency"] = df["total_orders"] / (df["customer_tenure_days"] + 1)
    df["avg_spend_per_order"] = df["avg_order_value"]

    # Segment-based encoding
    df["is_premium_segment"] = (df["customer_segment"].str.lower() == "premium").astype(int)

    logger.info(f" Customer features generated: {df.shape}")
    return df
