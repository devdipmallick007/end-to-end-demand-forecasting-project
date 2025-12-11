from models.train import train_model
from models.evaluate import evaluate_model
from models.forecast import generate_forecast
from utils.mlflow_utils import init_mlflow
import mlflow
from utils.logger import logger

if __name__ == "__main__":
    print("Starting full ML pipeline...")

    # Initialize MLflow
    init_mlflow()
    mlflow.set_experiment("store_brand_product_forecast")

    with mlflow.start_run(run_name="full_pipeline_run"):
        
        # Train model
        model, X_test, y_test = train_model()

        # Evaluate model
        mae, rmse = evaluate_model(model, X_test, y_test)

        # Log metrics to MLflow
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("rmse", rmse)

        # Also log metrics to our custom logger
        logger.info(f"Model Evaluation Metrics -> MAE: {mae:.4f}, RMSE: {rmse:.4f}")

        # Generate forecast
        generate_forecast(model)

        # Log additional parameters
        mlflow.log_param("prediction_horizon_days", 30)

    print("Pipeline executed successfully!")
    logger.info("Pipeline completed successfully.")
