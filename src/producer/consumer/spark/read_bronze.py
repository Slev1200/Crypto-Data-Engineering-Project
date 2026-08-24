from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("ReadBronzeCryptoData")
    .getOrCreate()
)

df = spark.read.json("data/bronze/price_ticks/*.jsonl")

print("SCHEMA:")
df.printSchema()

print("SAMPLE DATA:")
df.select(
    "coin_id",
    "event_time",
    "price_usd",
    "market_cap_usd",
    "total_volume_usd"
).show(10, truncate=False)

spark.stop()
