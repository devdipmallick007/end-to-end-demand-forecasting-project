import pandas as pd
from utils.logger import logger

def generate_order_items_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("🔹 Generating order_items features...")

    df = df.copy()
    df["total_price"] = df["quantity"] * df["unit_price"]
    df["avg_price_per_unit"] = df["total_price"] / (df["quantity"] + 1e-5)

    # Aggregations per product
    product_stats = df.groupby("product_id").agg({
        "quantity": ["mean", "sum"],
        "total_price": ["mean", "sum"]
    })
    product_stats.columns = ["avg_qty", "total_qty", "avg_revenue", "total_revenue"]
    product_stats.reset_index(inplace=True)

    logger.info(f" Order items features generated: {df.shape}")
    return product_stats
