import pandas as pd
from utils.logger import logger

def generate_inventory_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(" Generating inventory features...")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors='coerce')
    df["net_stock"] = df["stock_received"] - df["damaged_stock"]

    # Rolling or lag features can be added later when merged by date
    logger.info(f" Inventory features generated: {df.shape}")
    return df
