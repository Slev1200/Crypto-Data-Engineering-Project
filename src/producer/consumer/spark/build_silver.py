from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("BuildSilverCryptoData")
    .getOrCreate()
)

df = spark.read.json("data/bronze/price_ticks/*.jsonl")

silver_df = (
    df
    .select(
        col("event_id"),
        col("coin_id"),
        col("symbol"),
        col("name"),
        col("event_time").cast("timestamp").alias("event_time"),
        col("ingested_at").cast("timestamp").alias("ingested_at"),
        col("price_usd").cast("double").alias("price_usd"),
        col("market_cap_usd").cast("long").alias("market_cap_usd"),
        col("total_volume_usd").cast("long").alias("total_volume_usd"),
        col("price_change_percentage_24h").cast("double")
    )
    .dropDuplicates(["event_id"])
    .where(col("price_usd").isNotNull())
)

print("SILVER ROW COUNT:", silver_df.count())

silver_df.show(10, truncate=False)

silver_df.write.mode("overwrite").parquet("data/silver/price_ticks")

spark.stop()
