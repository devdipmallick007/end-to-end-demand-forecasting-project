# end-to-end-demand-forecasting-project
📦 Demand Forecasting System (End-to-End ML Pipeline)

An end-to-end production-ready Demand Forecasting System built with Python, XGBoost, MLflow, Redis caching, multithreading, SQL database integration, external API enrichment, and a modular data pipeline.
The system predicts daily sales quantity for Blinkit-style retail data using advanced feature engineering and time-series modeling.


🚀 Overview

This project implements a complete machine learning workflow:

✔ Multi-table SQL data ingestion
✔ Geocoding & weather API enrichment
✔ Redis caching for API optimization
✔ Multithreading for fast external API calls
✔ Table-wise cleaning and feature engineering
✔ Time-series feature engineering
✔ Full EDA dashboard support
✔ XGBoost time-series forecasting
✔ RMSE & MAE evaluation
✔ MLflow experiment tracking
✔ Centralized logging
✔ End-to-end pipeline execution using run_pipeline.py

🗂 Project Structure

demand_forecasting_system/
│
├── data/
│   ├── raw/
│   ├── cleaned/
│   ├── feature/
│   ├── final_data/
│   |── merge/
|   └── forecast/
│
├── logs/
│   └── pipeline.log
│
├── src/
│   ├── data_extraction/
│   ├── data_pipeline/
|   |   ├── data_clean/
│   |   ├── feature_engineering/
│   ├── models/
│   ├── utils/
|   ├── task/
|   ├── mlruns/
|   ├── config/
|   │ 
│   │──clean_data.py   
|   │──main_feature.py   
|   │──featch_data.py   
|   │──model.py   
|  
│
├── models/
│   ├── train.py
│   ├── evaluate.py
│   └── forecast.py
│
|── README.md
└── run_pipeline.py

🗄 1. Data Sources

The following SQL tables are used:

blinkit_customers

blinkit_delivery_performance

blinkit_marketing_performance

blinkit_order_items

blinkit_orders

blinkit_products

Plus one CSV file:

blinkit_inventory.csv

These collectively build the full operational dataset for forecasting.


🌐 2. Data Extraction + External API Enrichment
Geocoding API

Uses customer/store area information

Fetches latitude and longitude

Weather API

Fetches historical weather for each date

Temperature, humidity, rainfall, etc.

Redis Caching

Avoids repeated requests for same location+date

Boosts pipeline performance

Multithreading

Parallel API calls for large datasets

Improves speed during data enrichment


🧹 3. Table-wise Data Cleaning

Each table is cleaned using clean_data.py:

Handle missing values

Remove duplicates

Fix datatypes

Correct inconsistent values

Standardize date formats

Apply table-specific business rules

Cleaned outputs are stored in:

data/cleaned/

🏗 4. Feature Engineering
A. Table-wise Feature Engineering

Each cleaned table undergoes feature engineering:

Delivery performance metrics

Customer behavior features

Product-level attributes

Marketing indicators

Inventory transformations

Stored in:

data/feature/

B. Time-Series Feature Engineering (Global Merge)

Executed using main_feature.py after merging all tables:

Lag features: 1, 2, 3, 7, 14, 21, 28 days

Rolling means: 3, 7, 14, 28 days

Trend & seasonal signatures

Calendar features (month, day, week, holiday flags)

Product/store/category-level aggregations

Final dataset stored in:

data/final_data/



📊 5. Exploratory Data Analysis (EDA)

Performed after merging all features:

Time-series trend

Seasonal patterns

Product/category insights

Revenue & quantity distributions

Correlations

An optional Flask dashboard can be used for interactive EDA.



🤖 6. Modeling (XGBoost)

The forecasting model uses XGBoost Regressor:

Train/test split

Fit on engineered time-series features

Daily sales prediction

Future forecasting (7–30 days configurable)

The trained model is saved automatically.



📈 7. Model Evaluation

Two key evaluation metrics:

RMSE (Root Mean Squared Error)

MAE (Mean Absolute Error)

Both metrics are:

✔ Printed
✔ Logged locally
✔ Logged to MLflow



📦 8. MLflow Integration

MLflow is used for:

Parameters

Model type

Horizon days

Training configuration

Metrics

RMSE

MAE

Artifacts

Model pickle file

Forecast files

Optional charts

MLflow experiment:

store_brand_product_forecast


Helps compare models and maintain traceability.



📜 9. Logging System

Every stage logs:

Start/end timestamps

Info messages

Warning & error logs

MLflow sync status

Logs stored in:

logs/pipeline.log



▶️ 10. End-to-End Pipeline Execution

Run the full system with:

python run_pipeline.py


This triggers:

Data extraction

Geocoding + weather enrichment

Caching

Cleaning

Feature engineering

Merge

Time-series engineering

Modeling

Evaluation

Forecast generation

Logging

MLflow tracking


🧪 11. Requirements
pandas
numpy
sqlalchemy
requests
redis
xgboost
mlflow
scikit-learn
statsmodels
plotly
flask
pyyaml


Install:

pip install -r requirements.txt


📸 12. Future Enhancements

CI/CD with GitHub Actions

Automated retraining pipeline

Model registry + deployment pipeline

Real-time inference API

Feature store integration

🙌 
Devdip Mallick
