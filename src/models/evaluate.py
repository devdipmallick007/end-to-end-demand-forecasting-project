from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import mlflow

def evaluate_model(model, X_test, y_test):
    preds = model.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))

    print(f"MAE: {mae}")
    print(f"RMSE: {rmse}")

    # Log metrics
    mlflow.log_metric("mae", mae)
    mlflow.log_metric("rmse", rmse)

    return mae, rmse
