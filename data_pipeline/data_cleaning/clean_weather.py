import logging
import pandas as pd

def clean_weather_data(df_weather: pd.DataFrame) -> pd.DataFrame:
    df = df_weather.copy()
    logs = {}

    logs["initial_rows"] = len(df)
    logs["initial_missing"] = df.isnull().sum().to_dict()

    # Keep only relevant columns safely
    usable_cols = ["area", "date", "temperature", "precipitation"]
    df = df[[col for col in usable_cols if col in df.columns]].copy()

    # Drop duplicates (area + date defines unique record)
    before = len(df)
    df.drop_duplicates(subset=["area", "date"], inplace=True)
    logs["duplicates_removed"] = before - len(df)

    # Handle categorical columns
    if "area" in df.columns:
        df["area"] = df["area"].fillna("Unknown")

    # Numeric columns
    numeric_cols = [col for col in ["temperature", "precipitation"] if col in df.columns]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].fillna(df[col].mean())

    # Remove rows where date is missing
    before = len(df)
    df.dropna(subset=["date"], inplace=True)
    logs["rows_removed_missing_date"] = before - len(df)

    # Convert date dtype properly
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

    logs["final_rows"] = len(df)
    logs["rows_removed_total"] = logs["initial_rows"] - logs["final_rows"]
    logs["final_missing"] = df.isnull().sum().to_dict()

    # Logging report
    logging.info(" CLEANING REPORT: Weather Data")
    for k, v in logs.items():
        logging.info(f"{k}: {v}")

    return df
