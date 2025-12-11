import pandas as pd
from utils.logger import logger

def generate_customer_features(df: pd.DataFrame, orders_df: pd.DataFrame = None) -> pd.DataFrame:
    logger.info("Generating customer features...")

    df = df.copy()
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")

    # customer tenure
    df["customer_tenure_days"] = (pd.Timestamp.now().normalize() - df["registration_date"]).dt.days

    # frequency
    df["order_frequency"] = df["total_orders"] / (df["customer_tenure_days"] + 1)

    # avg spend
    df["avg_spend_per_order"] = df["avg_order_value"]

    # premium flag
    df["is_premium_segment"] = (df["customer_segment"].str.lower() == "premium").astype(int)

    # numeric encoded segment
    df["customer_segment_encoded"] = df["customer_segment"].astype("category").cat.codes

    # compute last order date if order table is provided
    if orders_df is not None:
        orders_df = orders_df.copy()
        orders_df["order_date"] = pd.to_datetime(orders_df["order_date"])
        last_order = (
            orders_df.groupby("customer_id")["order_date"].max().reset_index()
                      .rename(columns={"order_date": "last_order_date"})
        )

        df = df.merge(last_order, on="customer_id", how="left")
        df["days_since_last_order"] = (
            pd.Timestamp.now().normalize() - df["last_order_date"]
        ).dt.days
        df["days_since_last_order"] = df["days_since_last_order"].fillna(999)
    else:
        df["days_since_last_order"] = 999

    # area-level aggregates
    area_features = (
        df.groupby("area")
        .agg(
            customers_count_area=("customer_id", "nunique"),
            avg_customer_orders_area=("total_orders", "mean"),
            avg_customer_aov_area=("avg_order_value", "mean"),
            avg_customer_frequency_area=("order_frequency", "mean"),
            premium_segment_ratio=("is_premium_segment", "mean"),
            avg_days_since_last_order=("days_since_last_order", "mean"),
        )
        .reset_index()
    )

    logger.info(f"Customer features generated: {area_features.shape}")
    return area_features
