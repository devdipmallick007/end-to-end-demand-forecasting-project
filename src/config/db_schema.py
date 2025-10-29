# src/config/db_schema.py

# Table schemas
schemas = {
    "blinkit_customer_feedback": [
        "feedback_id", "order_id", "customer_id", "rating",
        "feedback_text", "feedback_category", "sentiment", "feedback_date"
    ],
    "blinkit_customers": [
        "customer_id", "customer_name", "email", "phone", "address",
        "area", "pincode", "registration_date", "customer_segment", "total_orders"
    ],
    "blinkit_delivery_performance": [
        "order_id", "delivery_partner_id", "promised_time", "actual_time",
        "delivery_time_minutes", "distance_km", "delivery_status", "reasons_if_delayed"
    ],
    "blinkit_marketing_performance": [
        "campaign_id", "campaign_name", "date", "target_audience", "channel",
        "impressions", "clicks", "conversions", "spend", "revenue_generated"
    ],
    "blinkit_order_items": [
        "order_id", "product_id", "quantity", "unit_price"
    ],
    "blinkit_orders": [
        "order_id", "customer_id", "order_date", "promised_delivery_time",
        "actual_delivery_time", "delivery_status", "order_total",
        "payment_method", "delivery_partner_id", "store_id"
    ],
    "blinkit_products": [
        "product_id", "product_name", "category", "brand", "price",
        "mrp", "margin_percentage", "shelf_life_days", "min_stock_level", "max_stock_level"
    ],

    # Add weather table here
    "blinkit_weather_data": [
        "area",
        "date",
        "temperature",
        "precipitation"
    ]
}



# -------------------------------
# Helper functions
# -------------------------------

def get_table_columns(table_name):
    """Return the list of columns for a given table."""
    if table_name not in schemas:
        raise ValueError(f"Table '{table_name}' not found in schemas")
    return schemas[table_name]


def get_all_tables():
    """Return a list of all table names in schemas."""
    return list(schemas.keys())


def validate_columns(table_name, columns):
    """
    Validate if the provided columns exist in the table schema.
    Returns True if all exist, else raises ValueError.
    """
    table_cols = get_table_columns(table_name)
    missing = [col for col in columns if col not in table_cols]
    if missing:
        raise ValueError(
            f"Columns {missing} not found in table '{table_name}'")
    return True
