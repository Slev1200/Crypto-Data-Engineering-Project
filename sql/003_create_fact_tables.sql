CREATE TABLE IF NOT EXISTS fact_price_tick (
    event_id TEXT PRIMARY KEY,
    coin_id TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    price_usd NUMERIC,
    market_cap_usd NUMERIC,
    total_volume_usd NUMERIC,
    price_change_percentage_24h NUMERIC
);

CREATE TABLE IF NOT EXISTS fact_daily_coin_metrics (
    coin_id TEXT NOT NULL,
    metric_date DATE NOT NULL,
    avg_price_usd NUMERIC,
    min_price_usd NUMERIC,
    max_price_usd NUMERIC,
    avg_market_cap_usd NUMERIC,
    avg_volume_usd NUMERIC,
    tick_count INTEGER,
    first_event_time TIMESTAMPTZ,
    last_event_time TIMESTAMPTZ,
    PRIMARY KEY (coin_id, metric_date)
);

INSERT INTO fact_daily_coin_metrics (
    coin_id,
    metric_date,
    avg_price_usd,
    min_price_usd,
    max_price_usd,
    avg_market_cap_usd,
    avg_volume_usd,
    tick_count,
    first_event_time,
    last_event_time
)
SELECT
    coin_id,
    DATE(event_time) AS metric_date,
    AVG(price_usd) AS avg_price_usd,
    MIN(price_usd) AS min_price_usd,
    MAX(price_usd) AS max_price_usd,
    AVG(market_cap_usd) AS avg_market_cap_usd,
    AVG(total_volume_usd) AS avg_volume_usd,
    COUNT(*) AS tick_count,
    MIN(event_time) AS first_event_time,
    MAX(event_time) AS last_event_time
FROM fact_price_tick
GROUP BY coin_id, DATE(event_time)
ON CONFLICT (coin_id, metric_date) DO UPDATE
SET
    avg_price_usd = EXCLUDED.avg_price_usd,
    min_price_usd = EXCLUDED.min_price_usd,
    max_price_usd = EXCLUDED.max_price_usd,
    avg_market_cap_usd = EXCLUDED.avg_market_cap_usd,
    avg_volume_usd = EXCLUDED.avg_volume_usd,
    tick_count = EXCLUDED.tick_count,
    first_event_time = EXCLUDED.first_event_time,
    last_event_time = EXCLUDED.last_event_time;