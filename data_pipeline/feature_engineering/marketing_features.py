import pandas as pd
from utils.logger import logger

def generate_marketing_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("🔹 Generating marketing features...")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors='coerce')

    df["ctr"] = df["clicks"] / (df["impressions"] + 1)
    df["conversion_rate"] = df["conversions"] / (df["clicks"] + 1)
    df["roi"] = df["revenue_generated"] / (df["spend"] + 1)
    df["campaign_duration_days"] = df.groupby("campaign_id")["date"].transform(lambda x: (x.max() - x.min()).days + 1)

    logger.info(f" Marketing features generated: {df.shape}")
    return df
