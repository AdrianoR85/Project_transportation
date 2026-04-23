from pyspark.sql import functions as F
from pyspark import pipelines as dp


@dp.materialized_view(
    name="transportation.silver.city",
    comment="Clean and stardardized products dimensions with business transformations.",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true", # Instead of only seeing the current state of the table, you can also query what changed between versions.
        "delta.autoOptimize.optimizeWrite": "true", # Ensures data is written efficiently, avoiding small file problems and improving performance from the beginning.
        "delta.autoOptimize.autoCompact": "true" # Automatically merges small files into larger ones to maintain efficient storage and consistent performance over time.
    }
)
def city_silver():
    
    # Extracting data from bronze table
    df_bronze = spark.read.table("transportation.bronze.city")
    
    # selecting required columns
    df_silver = df_bronze.select(
        F.col("city_id").alias("city_id"),
        F.col("city_name").alias("city_name"),
        F.col("ingest_datetime").alias("bronze_ingested_timestamp")
    )

    # creating a new column
    df_silver = df_silver.withColumn("silver_ingested_timestamp", F.current_timestamp())
    
    return df_silver