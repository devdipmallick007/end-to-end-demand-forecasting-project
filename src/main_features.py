# main_features.py
import os
import sys
import pandas as pd
import yaml
from utils.logger import logger
from utils.schema_validator import SchemaValidator

PROJECT_ROOT = r"D:\demand_forecasting_system"
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
FEATURE_DIR = os.path.join(SRC_DIR, "data_pipeline", "feature_engineering")
FEATURE_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "feature")
MERGE_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "merge")  # <--- new merge folder
FINAL_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "final_data")

# Add paths to sys.path
for path in [PROJECT_ROOT, SRC_DIR, FEATURE_DIR]:
    if path not in sys.path:
        sys.path.append(path)

# Import feature functions
from data_pipeline.feature_engineering.customer_features import generate_customer_features
from data_pipeline.feature_engineering.orders_features import generate_orders_features
from data_pipeline.feature_engineering.orders_items_features import generate_order_items_features
from data_pipeline.feature_engineering.products_features import generate_products_features
from data_pipeline.feature_engineering.finalize_store_product import generate_store_product_timeseries_features
from data_pipeline.feature_engineering.merge_and_aggregate import build_final_dataset


# Load configuration

logger.info("Loading configuration...")
config_path = os.path.join(FEATURE_DIR, "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

base_path = config["data_paths"]["base_path"]
logger.info("Configuration loaded successfully.")


# Load clean datasets

def load_data(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    logger.info(f"Loading: {path}")
    return pd.read_csv(path)

datasets = {}
for name, rel_path in config["data_paths"].items():
    if name == "base_path":
        continue
    full_path = os.path.join(base_path, rel_path)
    datasets[name] = load_data(full_path)
    logger.info(f"{name} loaded: {datasets[name].shape}")


# Feature Engineering
logger.info("Running individual feature engineering...")
features_map = {
    "customers": generate_customer_features(datasets["customers"]),
    "products": generate_products_features(datasets["products"]),
    "orders": generate_orders_features(datasets["orders"]),
    "order_items": generate_order_items_features(datasets["order_items"])
}

# Ensure folders exist
os.makedirs(FEATURE_OUTPUT_DIR, exist_ok=True)
os.makedirs(MERGE_OUTPUT_DIR, exist_ok=True)
os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)

# Save individual feature tables
for name, df in features_map.items():
    out_path = os.path.join(FEATURE_OUTPUT_DIR, f"{name}_features.csv")
    df.to_csv(out_path, index=False)
    logger.info(f"Saved feature table: {out_path}")


# Merge feature tables
logger.info("Building final joined dataset...")
merged_df = build_final_dataset(
    orders_df=features_map["orders"],
    order_items_df=features_map["order_items"],
    products_df=features_map["products"],
    customer_feature=features_map["customers"],
    weather_feature=None,
    output_path=None
)

# Save merged dataset in the merge folder
merge_output_path = os.path.join(MERGE_OUTPUT_DIR, "merged_store_product.csv")
merged_df.to_csv(merge_output_path, index=False)
logger.info(f"Merged dataset saved: {merge_output_path}")

# Time-Series Feature Engineering

logger.info("Running store-product time-series feature engineering...")
final_df = generate_store_product_timeseries_features(merged_df, FEATURE_DIR)

# Schema Validation

schema_path = os.path.join(FEATURE_DIR, "final_schema.yaml")
validator = SchemaValidator(schema_path)

try:
    final_df = validator.validate(final_df)
except Exception as e:
    logger.error(f"Schema validation failed: {e}")
    raise

# Save final dataset

final_output_path = os.path.join(FINAL_OUTPUT_DIR, "final_store_product.csv")
final_df.to_csv(final_output_path, index=False)
logger.info(f"Final dataset saved: {final_output_path}")

print("\nPIPELINE EXECUTION FINISHED")
