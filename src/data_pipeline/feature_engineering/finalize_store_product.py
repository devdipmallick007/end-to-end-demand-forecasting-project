# finalize_store_product.py
import pandas as pd
from utils.logger import logger
from utils.schema_validator import SchemaValidator
import os

def generate_store_product_timeseries_features(df: pd.DataFrame, feature_dir: str) -> pd.DataFrame:

    logger.info("Starting final store-product time-series feature generation...")

    # ===============================
    # 1. Ensure datatypes + sorting
    # ===============================
    df["order_date"] = pd.to_datetime(df["order_date"])
    df = df.sort_values(["store_id", "product_id", "order_date"])

    # ===============================
    # 2. Daily Aggregation
    # ===============================
    logger.info("Aggregating to store-product daily level...")

    daily_df = (
        df.groupby(["store_id", "product_id", "order_date"])
        .agg(
            daily_qty=("quantity", "sum"),
            daily_revenue=("total_price", "sum"),
            orders_count=("order_id", "nunique"),
            high_value_item_ratio=("high_value_item", "mean")
        )
        .reset_index()
    )

    daily_df["avg_unit_price"] = (
        daily_df["daily_revenue"] / daily_df["daily_qty"].replace(0, 1)
    )

    logger.info("Daily aggregation completed.")

    # ===============================
    # 3. Time-series Features
    # ===============================
    logger.info("Generating lag, rolling, cumulative features...")

    keys = ["store_id", "product_id"]

    # LAG FEATURES
    for lag in [1, 7, 14, 30]:
        daily_df[f"lag_{lag}_qty"] = daily_df.groupby(keys)["daily_qty"].shift(lag)

    # ROLLING FEATURES
    for window in [7, 14, 30]:
        daily_df[f"rolling_{window}_mean_qty"] = (
            daily_df.groupby(keys)["daily_qty"].shift(1).rolling(window, min_periods=1).mean()
        )
        daily_df[f"rolling_{window}_sum_qty"] = (
            daily_df.groupby(keys)["daily_qty"].shift(1).rolling(window, min_periods=1).sum()
        )

    # CUMULATIVE FEATURES
    daily_df["cumulative_qty"] = daily_df.groupby(keys)["daily_qty"].cumsum()
    daily_df["cumulative_revenue"] = daily_df.groupby(keys)["daily_revenue"].cumsum()

    # VOLATILITY
    daily_df["rolling_30_std_qty"] = (
        daily_df.groupby(keys)["daily_qty"].shift(1).rolling(30, min_periods=2).std()
    )

    # GROWTH RATE
    daily_df["qty_growth_rate"] = (
        (daily_df["daily_qty"] / daily_df["lag_1_qty"]) - 1
    ).replace([pd.NA, pd.NaT, float("inf"), -float("inf")], 0)

    # ===============================
    # 4. Calendar Features
    # ===============================
    logger.info("Adding calendar features...")

    daily_df["day"] = daily_df["order_date"].dt.day
    daily_df["month"] = daily_df["order_date"].dt.month
    daily_df["year"] = daily_df["order_date"].dt.year
    daily_df["day_of_week"] = daily_df["order_date"].dt.dayofweek
    daily_df["is_weekend"] = daily_df["day_of_week"].isin([5, 6]).astype(int)
    daily_df["is_month_start"] = daily_df["order_date"].dt.is_month_start.astype(int)
    daily_df["is_month_end"] = daily_df["order_date"].dt.is_month_end.astype(int)

    logger.info("Calendar feature engineering completed.")

    # ===============================
    # 5. SCHEMA VALIDATION
    # ===============================
    schema_path = os.path.join(feature_dir, "final_schema.yaml")
    logger.info(f"Validating final schema using: {schema_path}")

    validator = SchemaValidator(schema_path)
    daily_df = validator.validate(daily_df)

    logger.info(f"Final dataset shape: {daily_df.shape}")
    logger.info("Store-product time-series feature generation completed successfully.")

    return daily_df
