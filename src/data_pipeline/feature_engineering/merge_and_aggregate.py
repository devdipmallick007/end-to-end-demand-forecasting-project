import pandas as pd
import logging

logger = logging.getLogger("data_pipeline")
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')


def merge_tables(
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame,
    products_df: pd.DataFrame,
    customer_feature: pd.DataFrame = None,
    weather_df: pd.DataFrame = None  # WILL NOT BE USED (area invalid)
) -> pd.DataFrame:

    # === 1. Merge order_items → orders ===
    logger.info("Merging order_items with orders...")
    items_full = order_items_df.merge(
        orders_df,
        on='order_id',
        how='left',
        validate='m:1'
    )
    logger.info(f"Merged order_items + orders: {items_full.shape}")

    # === 2. Merge products ===
    logger.info("Merging products...")
    items_full = items_full.merge(
        products_df,
        on='product_id',
        how='left',
        validate='m:1'
    )
    logger.info(f"Merged products: {items_full.shape}")

    # === 3. CUSTOMER MERGE USING customer_id ONLY ===
    if customer_feature is not None:
        if "customer_id" in customer_feature.columns:
            logger.info("Merging customer features (customer_id-level)...")
            items_full = items_full.merge(
                customer_feature,
                on="customer_id",
                how="left",
                validate="m:1"
            )
            logger.info(f"Merged customer features: {items_full.shape}")
        else:
            logger.warning("Customer feature provided but missing 'customer_id'. Skipping merge.")
    else:
        logger.info("Customer feature not provided. Skipping customer merge.")

    # === 4. SKIP WEATHER MERGE — AREA IS INVALID ===
    if weather_df is not None:
        logger.warning("Weather merge skipped because AREA is invalid in your dataset.")
    else:
        logger.info("Weather feature not provided.")

    return items_full



def build_final_dataset(
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame,
    products_df: pd.DataFrame,
    customer_feature: pd.DataFrame = None,
    weather_feature: pd.DataFrame = None,
    output_path: str = None,
) -> pd.DataFrame:

    final_df = merge_tables(
        orders_df=orders_df,
        order_items_df=order_items_df,
        products_df=products_df,
        customer_feature=customer_feature,
        weather_df=weather_feature    # ignored intentionally
    )

    if output_path:
        final_df.to_csv(output_path, index=False)
        logger.info(f"Final dataset saved at: {output_path}")

    return final_df
