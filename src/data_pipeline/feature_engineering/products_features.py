import pandas as pd
import numpy as np
from utils.logger import logger


def generate_products_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Generating product features...")

    if df.empty:
        logger.warning("products dataframe is empty. Returning empty feature set.")
        return pd.DataFrame()

    df = df.copy()

     

    numeric_cols = [
        "price", "mrp", "margin_percentage", "shelf_life_days",
        "min_stock_level", "max_stock_level", "discount_percentage"
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.fillna({
        "discount_percentage": 0,
        "margin_percentage": 0,
        "shelf_life_days": df["shelf_life_days"].median(),
        "min_stock_level": 0,
        "max_stock_level": df["max_stock_level"].median(),
    }, inplace=True)

    # Profit value (per unit)
    df["profit_margin_value"] = df["price"] * (df["margin_percentage"] / 100)

    # How much cheaper than MRP (actual discount amount)
    df["discount_amount"] = (df["mrp"] - df["price"]).clip(lower=0)

    df["discount_flag"] = (df["discount_percentage"] > 0).astype(int)

    # Price vs MRP strength indicator
    df["price_mrp_ratio"] = df["price"] / (df["mrp"] + 1e-5)

    # 3. STOCK STABILITY FEATURES
   
    df["stock_range"] = (df["max_stock_level"] - df["min_stock_level"]).clip(lower=0)
    df["stock_volatility"] = df["stock_range"] / (df["min_stock_level"] + 1)

    # 4. CATEGORY / BRAND POSITIONING 
    df["is_top_brand"] = df["brand"].isin(df["brand"].value_counts().nlargest(5).index).astype(int)
    df["is_top_category"] = df["category"].isin(df["category"].value_counts().nlargest(5).index).astype(int)

    # 5. DEMAND-FORECASTING FLAGS

    df["high_margin_flag"] = (df["margin_percentage"] >= df["margin_percentage"].median()).astype(int)
    df["long_shelf_life_flag"] = (df["shelf_life_days"] >= df["shelf_life_days"].median()).astype(int)

    # Price segmentation
    df["is_premium_product"] = (df["price"] >= df["price"].quantile(0.75)).astype(int)
    df["is_budget_product"] = (df["price"] <= df["price"].quantile(0.25)).astype(int)

    # Profitability score (softmax normalized)
    df["profit_score"] = (df["profit_margin_value"] - df["profit_margin_value"].mean()) / (
        df["profit_margin_value"].std() + 1e-5
    )


    df.replace([np.inf, -np.inf], 0, inplace=True)
    df.fillna(0, inplace=True)

    logger.info(f"Product features generated: {df.shape}")
    return df
