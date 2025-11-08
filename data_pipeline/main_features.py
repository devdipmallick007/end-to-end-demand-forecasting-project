import os
import sys
import pandas as pd
import yaml
from datetime import datetime

# === 1. Define Project Paths ===
PROJECT_ROOT = r"D:\demand_forecasting_system"
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
PIPELINE_DIR = os.path.join(PROJECT_ROOT, "data_pipeline")
FEATURE_DIR = os.path.join(PIPELINE_DIR, "feature_engineering")
FEATURE_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "feature")  #  new folder for all features

# === 2. Add necessary directories to Python path ===
for path in [PROJECT_ROOT, SRC_DIR, PIPELINE_DIR, FEATURE_DIR]:
    if path not in sys.path:
        sys.path.append(path)

# === 3. Import Utilities and Feature Scripts ===
from utils.logger import logger
from feature_engineering.customer_features import generate_customer_features
from feature_engineering.inventory_features import generate_inventory_features
from feature_engineering.marketing_features import generate_marketing_features
from feature_engineering.orders_features import generate_orders_features
from feature_engineering.orders_items_features import generate_order_items_features
from feature_engineering.products_features import generate_products_features
from feature_engineering.weather_features import generate_weather_features
from feature_engineering.join_features import join_all_features

# === 4. Load Configuration ===
config_path = os.path.join(FEATURE_DIR, "config.yaml")
with open(config_path, "r") as file:
    config = yaml.safe_load(file)

base_path = config["data_paths"]["base_path"]

logger.info("Configuration loaded successfully.")
logger.info(f"Base path: {base_path}")

print("\n=== CONFIGURATION ===")
print(f"Base Path: {base_path}")
for key, val in config["data_paths"].items():
    print(f"  {key}: {val}")

# === 5. Utility Function to Load Data ===
def load_data(file_path: str) -> pd.DataFrame:
    """Loads a CSV file with validation and logging."""
    if not os.path.exists(file_path):
        logger.error(f" File not found: {file_path}")
        raise FileNotFoundError(f"Missing file: {file_path}")
    logger.info(f" Loading data from {file_path}")
    return pd.read_csv(file_path)

# === 6. Load All Cleaned Data ===
datasets = {}
for name, relative_path in config["data_paths"].items():
    if name == "base_path":
        continue
    path = os.path.join(base_path, relative_path)
    datasets[name] = load_data(path)
    logger.info(f"{name.capitalize()} Data Loaded: {datasets[name].shape}")

# === 7. Generate Features for Each Dataset ===
logger.info(" Starting Feature Engineering for all datasets...")

features_map = {
    "customers": generate_customer_features(datasets["customers"]),
    "products": generate_products_features(datasets["products"]),
    "orders": generate_orders_features(datasets["orders"]),
    "order_items": generate_order_items_features(datasets["order_items"]),
    "inventory": generate_inventory_features(datasets["inventory"]),
    "marketing": generate_marketing_features(datasets["marketing"]),
    "weather": generate_weather_features(datasets["weather"]),

}

# === 8. Save Each Feature File ===
os.makedirs(FEATURE_OUTPUT_DIR, exist_ok=True)

for name, df in features_map.items():
    output_path = os.path.join(FEATURE_OUTPUT_DIR, f"{name}_features.csv")
    df.to_csv(output_path, index=False)
    logger.info(f" {name.capitalize()} features saved at {output_path}")
    print(f" Saved: {output_path}")

# === 9. End of Pipeline ===
logger.info(" Feature Engineering Pipeline Completed Successfully.")
print("\nPipeline Execution Finished ")
