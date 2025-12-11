import pandas as pd, json, os, mlflow, pickle
from utils.logger import logger

def generate_forecast(model):
    logger.info("Generating forecast...")

    df = pd.read_csv("D:/demand_forecasting_system/data/final_data/final_store_product.csv")

    # Date features
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["year"] = df["order_date"].dt.year
    df["month"] = df["order_date"].dt.month
    df["week"] = df["order_date"].dt.isocalendar().week.astype(int)
    df["day"] = df["order_date"].dt.day
    df["dayofweek"] = df["order_date"].dt.dayofweek
    df["is_weekend"] = (df["dayofweek"] >= 5).astype(int)

    df = df.drop(columns=["order_date", "daily_qty"])

    # Load encoders & columns
    with open("artifacts/feature_columns.json", "r") as f:
        feature_cols = json.load(f)

    with open("artifacts/label_encoders.pkl", "rb") as f:
        encoders = pickle.load(f)

    X = df[feature_cols].copy()

    for c, le in encoders.items():
        X.loc[:, c] = le.transform(X[c])

    df["predicted_qty"] = model.predict(X)

    # Save forecast
    os.makedirs("data/forecasts", exist_ok=True)
    out_path = "D:/demand_forecasting_system/data/forecasts/store_product_forecast.csv"
    df.to_csv(out_path, index=False)

    mlflow.log_artifact(out_path)
    logger.info(f"Forecast saved: {out_path}")
