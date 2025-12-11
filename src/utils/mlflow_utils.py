import mlflow
import os

def init_mlflow():
    tracking_dir = r"D:/demand_forecasting_system/mlruns"
    os.makedirs(tracking_dir, exist_ok=True)

    mlflow.set_tracking_uri(f"file:///{tracking_dir}")
    mlflow.set_experiment("demand_forecasting")
    print(f"MLflow tracking URI set to: {mlflow.get_tracking_uri()}")