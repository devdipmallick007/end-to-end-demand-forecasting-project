import pandas as pd
from utils.logger import logger

def generate_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("🔹 Generating weather features...")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors='coerce')

    # Derived features
    df["is_rainy_day"] = (df["precipitation"] > 0).astype(int)
    df["temp_category"] = pd.cut(df["temperature"], bins=[-10, 15, 25, 35, 50],
                                 labels=["cold", "mild", "warm", "hot"])

    logger.info(f" Weather features generated: {df.shape}")
    return df
