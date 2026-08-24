import os

from pyspark.sql import SparkSession

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto")

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("WriteGoldToPostgres")
    .config("spark.jars", "/usr/share/java/postgresql.jar")
    .getOrCreate()
)

gold_df = spark.read.parquet(
    "data/gold/fact_daily_coin_metrics"
)

jdbc_url = f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}"

(
    gold_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "stg_daily_coin_metrics")
    .option("user", POSTGRES_USER)
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

print("Gold data written to PostgreSQL staging table")

spark.stop()
