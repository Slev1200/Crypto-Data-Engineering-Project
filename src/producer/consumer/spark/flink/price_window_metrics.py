import os

from pyflink.table import EnvironmentSettings, TableEnvironment

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto")

# Create a streaming TableEnvironment
settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(settings)

table_env.get_config().set("parallelism.default", "1")
table_env.get_config().set("table.local-time-zone", "UTC")
table_env.get_config().set("execution.checkpointing.interval", "30 s")


# Kafka source
table_env.execute_sql("""
CREATE TABLE crypto_price_ticks (
    event_id STRING,
    coin_id STRING,
    event_time STRING,
    price_usd DOUBLE,

    event_ts AS TO_TIMESTAMP(
        event_time,
        'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''
    ),

    WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
)
WITH (
    'connector' = 'kafka',
    'topic' = 'crypto.price_ticks',
    'properties.bootstrap.servers' = 'broker:9092',
    'properties.group.id' = 'crypto-flink-window-v1',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
)
""")

# deduplicate crypto_price_ticks above to keep data idempotent
# table_env.execute_sql("""
# CREATE TEMPORARY VIEW deduplicated_price_ticks AS
# SELECT
#     event_id,
#     coin_id,
#     event_ts,
#     price_usd
# FROM (
#     SELECT
#         event_id,
#         coin_id,
#         event_ts,
#         price_usd,
#         ROW_NUMBER() OVER (
#             PARTITION BY event_id
#             ORDER BY event_ts
#         ) AS row_num
#     FROM crypto_price_ticks
# )
# WHERE row_num = 1
# """)


# PostgreSQL sink
table_env.execute_sql(f"""
CREATE TABLE price_window_sink (
    coin_id STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),

    avg_price_usd DOUBLE,
    min_price_usd DOUBLE,
    max_price_usd DOUBLE,
    event_count INT,

    PRIMARY KEY (
        coin_id,
        window_start,
        window_end
    ) NOT ENFORCED
)
WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/{POSTGRES_DB}',
    'table-name' = 'fact_price_window_5m',
    'username' = '{POSTGRES_USER}',
    'password' = '{POSTGRES_PASSWORD}',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
)
""")


# Aggregate price events into 5-minute windows
table_env.execute_sql("""
INSERT INTO price_window_sink
SELECT
    coin_id,
    window_start,
    window_end,
    AVG(price_usd),
    MIN(price_usd),
    MAX(price_usd),
    CAST(COUNT(*) AS INT)
FROM TABLE(
    TUMBLE(
        TABLE crypto_price_ticks,
        DESCRIPTOR(event_ts),
        INTERVAL '5' MINUTES
    )
)
WHERE price_usd IS NOT NULL
GROUP BY
    coin_id,
    window_start,
    window_end
""")
