import os
import json
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

API_KEY = os.getenv("COINGECKO_API_KEY")
# KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "chainlink",
    "uniswap",
    "dogecoin",
    "cardano",
    "avalanche-2",
    "polygon",
    "litecoin",
]

TOPIC = "crypto.price_ticks"

BRONZE_DIR = Path("data/bronze/price_ticks")
BRONZE_DIR.mkdir(parents=True, exist_ok=True)


def make_event_id(coin_id: str, event_time: str) -> str:
    raw = f"coingecko|{coin_id}|{event_time}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def fetch_market_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"

    params = {
        "vs_currency": "usd",
        "ids": ",".join(COINS),
        "order": "market_cap_desc",
        "per_page": len(COINS),
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "1h,24h,7d",
    }

    headers = {
        "x-cg-demo-api-key": API_KEY
    }

    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def normalize_coin(row: dict) -> dict:
    ingested_at = datetime.now(timezone.utc).isoformat()
    event_time = row.get("last_updated") or ingested_at

    event = {
        "event_id": make_event_id(row["id"], event_time),
        "source": "coingecko",
        "coin_id": row["id"],
        "symbol": row.get("symbol"),
        "name": row.get("name"),
        "event_time": event_time,
        "ingested_at": ingested_at,
        "price_usd": row.get("current_price"),
        "market_cap_usd": row.get("market_cap"),
        "total_volume_usd": row.get("total_volume"),
        "price_change_percentage_24h": row.get("price_change_percentage_24h"),
        "payload": row,
    }

    return event


def write_bronze_backup(events):
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_file = BRONZE_DIR / f"price_ticks_{date_str}.jsonl"

    with output_file.open("a", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    sent_event_ids = set()

    while True:
        try:
            rows = fetch_market_data()
            events = [normalize_coin(row) for row in rows]

            sent_count = 0

            for event in events:
                if event["event_id"] in sent_event_ids:
                    print(f"Skipping duplicate event for {event['coin_id']}")
                    continue

                producer.send(
                    TOPIC,
                    key=event["coin_id"],
                    value=event,
                )

                sent_event_ids.add(event['event_id'])
                sent_count += 1

            producer.flush()
            write_bronze_backup(events)

            print(f"Sent {sent_count} events to Kafka")

        except Exception as e:
            print(f"Producer error: {e}")

        time.sleep(300)


if __name__ == "__main__":
    main()
