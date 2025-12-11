import pandas as pd
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import json, os, mlflow, pickle
from utils.logger import logger

def train_model():
    logger.info("Loading final dataset...")
    df = pd.read_csv("D:/demand_forecasting_system/data/final_data/final_store_product.csv")

    # -------------------------------
    # Feature Engineering
    # -------------------------------
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["year"] = df["order_date"].dt.year
    df["month"] = df["order_date"].dt.month
    df["week"] = df["order_date"].dt.isocalendar().week.astype(int)
    df["day"] = df["order_date"].dt.day
    df["dayofweek"] = df["order_date"].dt.dayofweek
    df["is_weekend"] = (df["dayofweek"] >= 5).astype(int)

    df = df.drop(columns=["order_date"])

    y = df["daily_qty"]
    X = df.drop(columns=["daily_qty"])

    # -------------------------------
    # Label Encoding
    # -------------------------------
    cat_cols = X.select_dtypes(include=['object']).columns.tolist()
    encoders = {}

    for c in cat_cols:
        le = LabelEncoder()
        X.loc[:, c] = le.fit_transform(X[c])
        encoders[c] = le

    os.makedirs("artifacts", exist_ok=True)

    with open("artifacts/label_encoders.pkl", "wb") as f:
        pickle.dump(encoders, f)

    with open("artifacts/feature_columns.json", "w") as f:
        json.dump(X.columns.tolist(), f)

    # -------------------------------
    # Train/Test Split
    # -------------------------------
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=False
    )

    # -------------------------------
    # Model Init
    # -------------------------------
    model = XGBRegressor(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=8,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=42
    )

    logger.info("Training model...")
    model.fit(X_train, y_train)

    # -------------------------------
    # Log Parameters to MLflow
    # -------------------------------
    mlflow.log_params(model.get_params())
    mlflow.log_param("categorical_columns", cat_cols)
    mlflow.log_param("train_shape", X_train.shape)
    mlflow.log_param("test_shape", X_test.shape)

    # -------------------------------
    # Log model to MLflow
    # -------------------------------
    mlflow.sklearn.log_model(model, "xgb_model")

    return model, X_test, y_test
