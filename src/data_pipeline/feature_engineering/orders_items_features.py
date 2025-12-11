import pandas as pd
import numpy as np
from utils.logger import logger


def generate_order_items_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Generating order_items features...")

    if df.empty:
        logger.warning("order_items dataframe is empty. Returning empty feature set.")
        return pd.DataFrame()

    df = df.copy()

   
    # 1. BASIC CLEANING
   
    df["quantity"] = df["quantity"].fillna(0).astype(float)
    df["unit_price"] = df["unit_price"].fillna(0).astype(float)

  
    # 2. CORE FINANCIAL FEATURES
  
    df["total_price"] = df["quantity"] * df["unit_price"]

    # More stable price-per-unit (avoid divide by zero)
    df["avg_price_per_unit"] = np.where(
        df["quantity"] > 0,
        df["total_price"] / df["quantity"],
        df["unit_price"]  # fallback
    )

    # 3. FORECASTING BEHAVIORAL FLAGS

    df["is_bulk_order"] = (df["quantity"] >= 5).astype(int)

    # dynamic high-value threshold (not median)
    price_threshold = df["unit_price"].quantile(0.75)
    df["high_value_item"] = (df["unit_price"] >= price_threshold).astype(int)

    # 4. ITEM-LEVEL REVENUE & PROFIT ESTIMATIONS
   
    df["item_revenue"] = df["total_price"]
    df["item_cost_estimation"] = df["unit_price"] * 0.7  # placeholder, replaced after product join
    df["item_profit_estimation"] = df["item_revenue"] - df["item_cost_estimation"]

    df.replace([np.inf, -np.inf], 0, inplace=True)
    df.fillna(0, inplace=True)

    logger.info(f"order_items features generated: {df.shape}")
    return df
