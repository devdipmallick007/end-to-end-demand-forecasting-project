# data_pipeline/feature_engineering/join_features.py

import pandas as pd
from utils.logger import logger

def join_all_features(
    orders_df,
    order_items_df,
    customers_df,
    products_df,
    inventory_df,
    marketing_df,
    weather_df
):
    logger.info("🔗 Starting feature joining pipeline...")

    # 1️⃣ Join orders with order_items
    df = pd.merge(
        orders_df,
        order_items_df,
        on="order_id",
        how="left"
    )

    logger.info(f"Step 1 - Orders + Order Items: {df.shape}")

    # 2️⃣ Join customer details
    df = pd.merge(
        df,
        customers_df,
        on="customer_id",
        how="left"
    )

    logger.info(f"Step 2 - Added Customer Info: {df.shape}")

    # 3️⃣ Join product details
    df = pd.merge(
        df,
        products_df,
        on="product_id",
        how="left"
    )

    logger.info(f"Step 3 - Added Product Info: {df.shape}")

    # 4️⃣ Join inventory (match product & order_date)
    df = pd.merge(
        df,
        inventory_df,
        left_on=["product_id", "order_date"],
        right_on=["product_id", "date"],
        how="left"
    ).drop(columns=["date"], errors="ignore")

    logger.info(f"Step 4 - Added Inventory Info: {df.shape}")

    # 5️⃣ Join marketing (date-level)
    df = pd.merge(
        df,
        marketing_df,
        left_on="order_date",
        right_on="date",
        how="left"
    ).drop(columns=["date"], errors="ignore")

    logger.info(f"Step 5 - Added Marketing Info: {df.shape}")

    # 6️⃣ Join weather (area + date)
    df = pd.merge(
        df,
        weather_df,
        left_on=["area", "order_date"],
        right_on=["area", "date"],
        how="left"
    ).drop(columns=["date"], errors="ignore")

    logger.info(f"Step 6 - Added Weather Info: {df.shape}")

    # 7️⃣ Final cleanup
    df = df.drop_duplicates(subset=["order_id", "product_id"], keep="first")

    logger.info(f"✅ Final dataset ready: {df.shape}")
    return df
def join_all_features(
    orders_df,
    order_items_df,
    customers_df,
    products_df,
    inventory_df,
    marketing_df,
    weather_df
):
    logger.info("🔗 Starting feature joining pipeline...")

    # 1️⃣ Join order_items → orders (one-to-many)
    df = pd.merge(order_items_df, orders_df, on="order_id", how="left")

    # 2️⃣ Join with products (product_id)
    df = pd.merge(df, products_df, on="product_id", how="left")

    # 3️⃣ Join with customers (customer_id)
    df = pd.merge(df, customers_df, on="customer_id", how="left")

    # 4️⃣ Join with inventory (product_id + date)
    df = pd.merge(df, inventory_df, on=["product_id", "date"], how="left")

    # 5️⃣ Join with weather (area + date)
    df = pd.merge(df, weather_df, on=["area", "date"], how="left")

    # 6️⃣ Marketing (use date + maybe channel if available)
    if "date" in marketing_df.columns:
        df = pd.merge(df, marketing_df, on="date", how="left")

    logger.info(f"✅ Final merged dataset shape: {df.shape}")
    return df
