import pandas as pd
import numpy as np
from utils.logger import logger


def generate_orders_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Generating orders features...")

    df = df.copy()


    # 1. STANDARD DATETIME CLEANING

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["promised_delivery_time"] = pd.to_datetime(df["promised_delivery_time"], errors="coerce")
    df["actual_delivery_time"] = pd.to_datetime(df["actual_delivery_time"], errors="coerce")

    # 2. BASIC TIME FEATURES

    df["order_day"] = df["order_date"].dt.day
    df["order_week"] = df["order_date"].dt.isocalendar().week.astype(int)
    df["order_month"] = df["order_date"].dt.month
    df["order_year"] = df["order_date"].dt.year
    df["day_of_week"] = df["order_date"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["order_hour"] = df["order_date"].dt.hour
    df["is_peak_hour"] = df["order_hour"].between(18, 22).astype(int)

    # 3. DELIVERY FEATURES

    df["delivery_delay_hrs"] = df["delivery_delay_hrs"].fillna(0)
    df["on_time_delivery"] = (df["delivery_delay_hrs"] <= 0).astype(int)

    df["actual_delivery_day"] = df["actual_delivery_time"].dt.floor("D")
    df["promised_delivery_day"] = df["promised_delivery_time"].dt.floor("D")

    df["delivery_diff_days"] = (
        (df["actual_delivery_time"] - df["promised_delivery_time"])
        .dt.total_seconds() / 86400
    ).fillna(0)

    df["delivery_speed_hrs"] = (
        (df["actual_delivery_time"] - df["order_date"])
        .dt.total_seconds() / 3600
    ).fillna(0)


    # 4. PAYMENT FEATURES

    df["payment_method"] = df["payment_method"].astype(str).str.lower()
    df["is_cod"] = (df["payment_method"] == "cod").astype(int)


    # 5. ORDER VALUE FEATURES
    
    df["order_total"] = df["order_total"].fillna(0)
    median_order_value = df["order_total"].median()
    df["is_high_value_order"] = (df["order_total"] > median_order_value).astype(int)
    df["log_order_total"] = (df["order_total"] + 1).apply(np.log)
    df["daily_order_flag"] = 1

 
    # 7. MONTH START & END FLAGS
   
    df["is_month_start"] = (df["order_date"].dt.day == 1).astype(int)
    df["is_month_end"] = (df["order_date"].dt.day >= df["order_date"].dt.days_in_month - 2).astype(int)

    logger.info(f"Orders features generated successfully: {df.shape}")
    return df
