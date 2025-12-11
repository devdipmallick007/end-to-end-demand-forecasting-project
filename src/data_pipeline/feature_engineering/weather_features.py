import pandas as pd
from utils.logger import logger

def generate_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Generating weather features...")

    df = df.copy()
    
    # --- Basic cleanup ---
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["temperature"] = df["temperature"].fillna(df["temperature"].median())
    df["precipitation"] = df["precipitation"].fillna(0)

    # --- Binary weather flags ---
    df["is_rainy_day"] = (df["precipitation"] > 0).astype(int)
    df["is_heavy_rain"] = (df["precipitation"] > df["precipitation"].quantile(0.90)).astype(int)
    df["is_hot_day"] = (df["temperature"] > 30).astype(int)
    df["is_cold_day"] = (df["temperature"] < 15).astype(int)

    # --- Temperature category buckets ---
    df["temp_category"] = pd.cut(
        df["temperature"],
        bins=[-50, 10, 20, 30, 50],
        labels=["very_cold", "mild", "warm", "hot"],
        include_lowest=True
    )

    # --- Weather intensity ---
    df["rain_intensity"] = pd.cut(
        df["precipitation"],
        bins=[0, 2, 10, 50, 500],
        labels=["none", "light", "moderate", "heavy"],
        include_lowest=True
    )


    df["temp_above_avg"] = (df["temperature"] > df["temperature"].mean()).astype(int)
    df["temp_deviation"] = df["temperature"] - df["temperature"].mean()

    # --- Date-based seasonality flags (important for demand) ---
    df["month"] = df["date"].dt.month
    df["day_of_week"] = df["date"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

    logger.info(f"Weather features generated: {df.shape}")
    return df
