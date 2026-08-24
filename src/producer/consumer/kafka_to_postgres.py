import os
import json
import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "55432"))

TOPIC = "crypto.price_ticks"


def get_connection():
    print(
        f"Connecting to host={POSTGRES_HOST}, "
        f"port={POSTGRES_PORT}, database={POSTGRES_DB}, "
        f"user={POSTGRES_USER}"
    )
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def insert_event(conn, event):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_crypto_price_events (
                event_id,
                coin_id,
                source,
                event_time,
                ingested_at,
                payload
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
            """,
            (
                event["event_id"],
                event["coin_id"],
                event["source"],
                event["event_time"],
                event["ingested_at"],
                json.dumps(event["payload"]),
            ),
        )

        cur.execute(
            """
            INSERT INTO fact_price_tick (
                event_id,
                coin_id,
                symbol,
                name,
                event_time,
                ingested_at,
                price_usd,
                market_cap_usd,
                total_volume_usd,
                price_change_percentage_24h
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
            """,
            (
                event["event_id"],
                event["coin_id"],
                event["symbol"],
                event["name"],
                event["event_time"],
                event["ingested_at"],
                event["price_usd"],
                event["market_cap_usd"],
                event["total_volume_usd"],
                event["price_change_percentage_24h"],
            ),
        )

    conn.commit()


def main():
    conn = get_connection()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="crypto-postgres-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    for message in consumer:
        event = message.value
        insert_event(conn, event)
        print(f"Inserted event {event['event_id']} for {event['coin_id']}")


if __name__ == "__main__":
    main()
