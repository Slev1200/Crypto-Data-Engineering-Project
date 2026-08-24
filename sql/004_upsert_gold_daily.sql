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
    metric_date,
    avg_price_usd,
    min_price_usd,
    max_price_usd,
    avg_market_cap_usd,
    avg_volume_usd,
    tick_count,
    first_event_time,
    last_event_time
FROM stg_daily_coin_metrics

ON CONFLICT (coin_id, metric_date)
DO UPDATE SET
    avg_price_usd = EXCLUDED.avg_price_usd,
    min_price_usd = EXCLUDED.min_price_usd,
    max_price_usd = EXCLUDED.max_price_usd,
    avg_market_cap_usd = EXCLUDED.avg_market_cap_usd,
    avg_volume_usd = EXCLUDED.avg_volume_usd,
    tick_count = EXCLUDED.tick_count,
    first_event_time = EXCLUDED.first_event_time,
    last_event_time = EXCLUDED.last_event_time;