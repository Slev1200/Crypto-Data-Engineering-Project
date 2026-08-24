CREATE TABLE IF NOT EXISTS fact_price_window_5m (
    coin_id TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,

    avg_price_usd NUMERIC,
    min_price_usd NUMERIC,
    max_price_usd NUMERIC,
    event_count INTEGER,

    PRIMARY KEY (coin_id, window_start, window_end)
);