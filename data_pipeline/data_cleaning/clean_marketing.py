import logging
import pandas as pd


def clean_marketing_data(df_marketing: pd.DataFrame) -> pd.DataFrame:
    df = df_marketing.copy()
    logs = {}

    logs["initial_rows"] = len(df)

    # Keep relevant columns
    usable_cols = [
        "campaign_id", "campaign_name", "date",
        "channel", "impressions", "clicks",
        "conversions", "spend", "revenue_generated"
    ]
    df = df[usable_cols].copy()

    # Drop duplicates by campaign + date
    before = len(df)
    df.drop_duplicates(subset=["campaign_id", "date"], inplace=True)
    logs["duplicates_removed"] = before - len(df)

    # Track missing before cleaning
    logs["missing_before"] = df.isnull().sum().to_dict()

    # Fill missing categorical values
    df["campaign_name"] = df["campaign_name"].fillna("Unknown")
    df["channel"] = df["channel"].fillna("Unknown")

    # Fill missing numeric values
    numeric_cols = ["impressions", "clicks", "conversions", "spend", "revenue_generated"]
    for col in numeric_cols:
        df[col] = df[col].fillna(0)

    # Remove rows where campaign_id is missing
    before = len(df)
    df.dropna(subset=["campaign_id"], inplace=True)
    logs["rows_removed_missing_id"] = before - len(df)

    # Fix dtypes
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
    df["campaign_id"] = df["campaign_id"].astype("Int64")

    for col in numeric_cols:
        df[col] = df[col].astype("float64")

    logs["final_rows"] = len(df)
    logs["rows_removed_total"] = logs["initial_rows"] - logs["final_rows"]

    # Logging entry
    logging.info("CLEANING REPORT: blinkit_marketing_performance")
    for k, v in logs.items():
        logging.info(f"{k}: {v}")

    return df
