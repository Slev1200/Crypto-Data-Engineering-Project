from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    avg,
    min,
    max,
    count,
    first,
    last,
)

spark = (
    SparkSession.builder
    .appName("BuildDailyCoinMetrics")
    .getOrCreate()
)

bronze_path = "data/bronze/price_ticks/*.jsonl"

df = spark.read.json(bronze_path)

clean_df = (
    df
    .select(
        col("event_id"),
        col("coin_id"),
        col("symbol"),
        col("name"),
        col("event_time").cast("timestamp").alias("event_time"),
        col("ingested_at").cast("timestamp").alias("ingested_at"),
        col("price_usd").cast("double").alias("price_usd"),
        col("market_cap_usd").cast("double").alias("market_cap_usd"),
        col("total_volume_usd").cast("double").alias("total_volume_usd"),
        col("price_change_percentage_24h").cast(
            "double").alias("price_change_percentage_24h"),
    )
    .dropDuplicates(["event_id"])
    .where(col("price_usd").isNotNull())
)

daily_df = (
    clean_df
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

daily_df.write.mode("overwrite").parquet("data/gold/fact_daily_coin_metrics")

spark.stop()
