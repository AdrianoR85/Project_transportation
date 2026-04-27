from pyspark import pipelines as dp
from pyspark.sql import functions as F



start_date = spark.conf.get("start_date")
end_date = spark.conf.get("end_date")

@dp.materialized_view(
    name="transportation.silver.calendar",
    comment="Calendar dimension with comprehensive date attributes and Brazilian Holidays (2020-2030)",
    table_properties={
        "quality": "transportation.silver.calendar",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
)
def calendar():
    df = spark.sql(
        f"""
        SELECT explode(sequence(
            to_date('{start_date}'),
            to_date('{end_date}'),
            interval 1 days
        )) as date
      """
    )

    # Creating date key in YYYYMMDD format
    df = df.withColumn(
        "date_key", F.date_format(F.col("date"), "yyyyMMdd").cast("int")
    )
    
    # Creating columns for year, month, quarter numerical values
    df = (
        df.withColumn("month", F.month(F.col("date")))
        .withColumn("year", F.year(F.col("date")))
        .withColumn("quarter", F.quarter(F.col("date")))
    )

    # Creating columns for day of month, day of week, day of week abbreviation, day of week number
    df = (
        df.withColumn("day_of_month", F.dayofmonth(F.col("date")))
        .withColumn("day_of_week", F.date_format(F.col("date"), "EEEE"))
        .withColumn("day_of_week_abbr", F.date_format(F.col("date"), "EEE"))
        .withColumn("day_of_week_num", F.dayofweek(F.col("date")))
    )

    # Creating columns for month name, month year, quarter year.
    df = (
        df.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))
        .withColumn(
            "month_year",
            F.concat(F.date_format(F.col("date"), "MMMM"), F.lit(" "), F.col("year"))
        )
        .withColumn(
            "quarter_year",
            F.concat(F.lit("Q"), F.col("quarter"), F.lit(" "), F.col("year"))
        )
    )

    # Creating columns for week of year and day of year
    df = df.withColumn(
        "week_of_year", F.weekofyear(F.col("date"))
        ).withColumn("day_of_year", F.dayofyear(F.col("date")))
    
    # Creating columns for weekday and weekend 
    df = df.withColumn(
        "is_weekend", 
        F.when(F.col("day_of_week_num").isin([1,7]), True).otherwise(False) 
    ).withColumn(
        "is_weekday", 
        F.when(F.col("day_of_week_num").isin([1,7]), False).otherwise(True) 
    )

    # Creating columns for holidays in Brazil
    df = df.withColumn(
        "holiday_name",
        F.when(
            (F.col("month") == 4) & (F.col("day_of_month") == 21), F.lit(" Tiradentes Day")
        ).when(
            (F.col("month") == 5) & (F.col("day_of_month") == 1), F.lit(" Labor Day")
        ).when(
            (F.col("month") == 9) & (F.col("day_of_month") == 7), F.lit("Independence Day")
        ).when(
            (F.col("month") == 10) & (F.col("day_of_month") == 12), F.lit("Our Lady of Aparecida day")
        ).when(
            (F.col("month") == 11) & (F.col("day_of_month") == 2), F.lit("All Saints Day")   
        ).when(
            (F.col("month") == 11) & (F.col("day_of_month") == 15), F.lit("Republic Day")    
        ).when(
            (F.col("month") == 12) & (F.col("day_of_month") == 25), F.lit("Christmas")
        ).otherwise(None)
    ).withColumn(
        "is_holiday",
        F.when(F.col("holiday_name").isNotNull(), True).otherwise(False)
    )
    
    # Creating silver_processed_timestamp
    df = df.withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )

    # Selecting columns
    df_silver = df.select(
    "date",
    "date_key",
    "year",
    "month",
    "day_of_month",
    "day_of_week",
    "day_of_week_abbr",
    "month_name",
    "month_year",
    "quarter",
    "quarter_year",
    "week_of_year",
    "day_of_year",
    "is_weekday",
    "is_weekend",
    "is_holiday",
    "holiday_name",
    "silver_processed_timestamp"
    )

    return df_silver
