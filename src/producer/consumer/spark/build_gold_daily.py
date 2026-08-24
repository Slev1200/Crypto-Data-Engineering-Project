from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    avg,
    min,
    max,
    count,
)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("BuildGoldDailyMetrics")
    .getOrCreate()
)

silver_df = spark.read.parquet("data/silver/price_ticks")

gold_df = (
    silver_df
    .withColumn("metric_date", to_date(col("event_time")))
    .groupBy("coin_id", "metric_date")
    .agg(
        avg("price_usd").alias("avg_price_usd"),
        min("price_usd").alias("min_price_usd"),
        max("price_usd").alias("max_price_usd"),
        avg("market_cap_usd").alias("avg_market_cap_usd"),
        avg("total_volume_usd").alias("avg_volume_usd"),
        count("*").alias("tick_count"),
        min("event_time").alias("first_event_time"),
        max("event_time").alias("last_event_time"),
    )
)

gold_df.orderBy("metric_date", "coin_id").show(20, truncate=False)

gold_df.write.mode("overwrite").parquet(
    "data/gold/fact_daily_coin_metrics"
)

spark.stop()
