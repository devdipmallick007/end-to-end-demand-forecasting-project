import pandas as pd
from utils.logger import logger

def generate_products_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("🔹 Generating product features...")

    df = df.copy()
    df["discount_percentage"] = df["discount_percentage"].fillna(0)
    df["profit_margin_value"] = df["price"] * (df["margin_percentage"] / 100)
    df["discount_flag"] = (df["discount_percentage"] > 0).astype(int)

    # Stock level spread
    df["stock_range"] = df["max_stock_level"] - df["min_stock_level"]

    logger.info(f" Product features generated: {df.shape}")
    return df
