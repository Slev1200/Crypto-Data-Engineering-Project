CREATE TABLE IF NOT EXISTS raw_crypto_price_events (
   event_id TEXT PRIMARY KEY,
   coin_id TEXT NOT NULL,
   source TEXT NOT NULL,
   event_time TIMESTAMPTZ NOT NULL,
   ingested_at TIMESTAMPTZ NOT NULL,
   payload JSONB NOT NULL

);