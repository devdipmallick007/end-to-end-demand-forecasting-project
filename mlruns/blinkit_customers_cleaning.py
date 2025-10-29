import pandas as pd
import numpy as np
import mlflow
from utils.logger import get_logger

logger = get_logger("customers_cleaning", log_file="logs/customers_cleaning.log")

def clean_blinkit_customers(df_customers: pd.DataFrame) -> pd.DataFrame:
    """
    Clean blinkit_customers data
    Steps:
      - Remove duplicates
      - Handle missing values
      - Validate data types
      - Apply basic sanity checks
    """
    with mlflow.start_run(run_name="blinkit_customers_cleaning"):
        try:
            logger.info("Starting Blinkit Customers cleaning process...")

            # 1️⃣ Drop duplicates
            before = len(df_customers)
            df_customers = df_customers.drop_duplicates(subset="customer_id", keep="first")
            after = len(df_customers)
            logger.info(f"Removed {before - after} duplicate records.")

            # 2️⃣ Handle missing values
            missing_summary = df_customers.isnull().sum()
            logger.info(f"Missing values before cleaning:\n{missing_summary}")

            # Fill missing area with 'Unknown'
            if 'area' in df_customers.columns:
                df_customers['area'] = df_customers['area'].fillna('Unknown')

            # Fill numeric columns with median
            for col in df_customers.select_dtypes(include=[np.number]).columns:
                median_val = df_customers[col].median()
                df_customers[col] = df_customers[col].fillna(median_val)

            logger.info("Missing values handled successfully.")

            # 3️⃣ Type validation
            if 'customer_id' in df_customers.columns:
                df_customers['customer_id'] = df_customers['customer_id'].astype(int)

            # 4️⃣ Sanity check
            invalid = df_customers[df_customers['total_orders'] < 0]
            if not invalid.empty:
                logger.warning(f"Found {len(invalid)} invalid negative order records, fixing...")
                df_customers.loc[df_customers['total_orders'] < 0, 'total_orders'] = 0

            mlflow.log_metric("rows_after_cleaning", len(df_customers))
            mlflow.log_artifact("logs/customers_cleaning.log")
            logger.info("Blinkit Customers cleaning completed successfully.")

            return df_customers

        except Exception as e:
            logger.error(f"Error during cleaning Blinkit Customers: {str(e)}")
            mlflow.log_param("status", "failed")
            raise


