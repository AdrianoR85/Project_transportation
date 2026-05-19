# ============================================================================
# CRM Customer - Dimension Table (already standardized, minor cleanup)
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp


# Already standardized naming, just cleanup and select
@dp.materialized_view(
    name="bike_store.silver.crm_customer",
    comment="Cleaned and standardized CRM Customer",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def crm_customer_silver():
    df_bronze = spark.read.table("bike_store.bronze.crm_customer")
    
    df_silver = df_bronze.select(
        col("cst_id").alias("customer_id"),
        col("cst_key").alias("customer_key"),
        col("cst_firstname").alias("first_name"),
        col("cst_lastname").alias("last_name"),
        col("cst_marital_status").alias("marital_status"),
        col("cst_gndr").alias("gender"),
        col("cst_create_date").alias("created_date")
    )

    # Data quality: Remove records with null customer_id
    df_silver = df_silver.filter(col("customer_id").isNotNull())

    # Add Silver metadata
    df_silver = df_silver.withColumn("ingest_timestamp", current_timestamp())

    return df_silver
