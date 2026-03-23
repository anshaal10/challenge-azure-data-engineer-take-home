"""
Utility functions for Blue Owls data pipeline.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, coalesce, lit


def handle_nulls(df: DataFrame, column: str, default_value=None) -> DataFrame:
    """
    Replace null and NaN values in a column with a default.
    
    Args:
        df: Input DataFrame
        column: Column name to process
        default_value: Value to use for nulls (default: keep as null)
        
    Returns:
        DataFrame with nulls replaced
    """
    if default_value is None:
        return df
    
    return df.withColumn(
        column,
        when(isnull(col(column)) | isnan(col(column)), lit(default_value))
        .otherwise(col(column))
    )


def dedup_on_keys(df: DataFrame, keys: list) -> DataFrame:
    """
    Remove duplicates keeping first occurrence as defined by natural order.
    
    Args:
        df: Input DataFrame
        keys: List of columns that form the natural key
        
    Returns:
        Deduplicated DataFrame
    """
    return df.dropDuplicates(keys)


def add_metadata_columns(df: DataFrame, source_endpoint: str) -> DataFrame:
    """
    Add standard metadata columns to bronze-layer data.
    
    Args:
        df: Input DataFrame
        source_endpoint: Source API endpoint
        
    Returns:
        DataFrame with metadata columns added
    """
    from datetime import datetime
    
    return df \
        .withColumn("_ingested_at", lit(datetime.utcnow().isoformat())) \
        .withColumn("_source_endpoint", lit(source_endpoint))


# Schema definitions for type casting
SCHEMAS = {
    "orders": {
        "order_id": "string",
        "customer_id": "string",
        "order_status": "string",
        "order_purchase_timestamp": "timestamp",
        "order_approved_at": "timestamp",
        "order_delivered_carrier_date": "timestamp",
        "order_delivered_customer_date": "timestamp",
        "order_estimated_delivery_date": "date",
    },
    "order_items": {
        "order_id": "string",
        "order_item_id": "integer",
        "product_id": "string",
        "seller_id": "string",
        "shipping_limit_date": "timestamp",
        "price": "decimal(10,2)",
        "freight_value": "decimal(10,2)",
    },
    "customers": {
        "customer_id": "string",
        "customer_unique_id": "string",
        "customer_zip_code_prefix": "string",
        "customer_city": "string",
        "customer_state": "string",
    },
    "products": {
        "product_id": "string",
        "product_category_name": "string",
        "product_name_length": "integer",
        "product_description_length": "integer",
        "product_photos_qty": "integer",
        "product_weight_g": "decimal(10,2)",
        "product_length_cm": "decimal(10,2)",
        "product_height_cm": "decimal(10,2)",
        "product_width_cm": "decimal(10,2)",
    },
    "sellers": {
        "seller_id": "string",
        "seller_zip_code_prefix": "string",
        "seller_city": "string",
        "seller_state": "string",
    },
    "payments": {
        "order_id": "string",
        "payment_sequential": "integer",
        "payment_type": "string",
        "payment_installments": "integer",
        "payment_value": "decimal(10,2)",
    },
}


def get_natural_keys(table_name: str) -> list:
    """
    Get the list of columns forming the natural key for a table.
    
    Args:
        table_name: Name of the table
        
    Returns:
        List of column names
    """
    natural_keys = {
        "orders": ["order_id"],
        "order_items": ["order_id", "order_item_id"],
        "customers": ["customer_id"],
        "products": ["product_id"],
        "sellers": ["seller_id"],
        "payments": ["order_id", "payment_sequential"],
    }
    return natural_keys.get(table_name, [])
