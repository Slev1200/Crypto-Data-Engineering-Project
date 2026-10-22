/* View #1 */

CREATE OR REPLACE VIEW vw_daily_coin_performance AS
SELECT
    f.coin_id,
    d.symbol,
    d.name,
    d.sector,
    f.metric_date,
    f.avg_price_usd,
    f.min_price_usd,
    f.max_price_usd,
    f.avg_market_cap_usd,
    f.avg_volume_usd,
    f.tick_count,
    f.first_event_time,
    f.last_event_time,

    f.max_price_usd - f.min_price_usd AS daily_price_range,

    LAG(f.avg_price_usd) OVER (
        PARTITION BY f.coin_id
        ORDER BY f.metric_date
    ) AS previous_day_avg_price

FROM fact_daily_coin_metrics f
LEFT JOIN dim_coin d
    ON f.coin_id = d.coin_id;

/* View #2 */
CREATE OR REPLACE VIEW vw_daily_coin_analytics AS
SELECT
    *,
    CASE
        WHEN previous_day_avg_price IS NOT NULL
             AND previous_day_avg_price <> 0
        THEN
            ((avg_price_usd - previous_day_avg_price)
             / previous_day_avg_price) * 100
    END AS daily_price_change_pct
FROM vw_daily_coin_performance;

/* View #3 */
CREATE OR REPLACE VIEW vw_latest_streaming_metrics AS
SELECT
    coin_id,
    window_start,
    window_end,
    avg_price_usd,
    min_price_usd,
    max_price_usd,
    event_count
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY coin_id
            ORDER BY window_end DESC
        ) AS row_num
    FROM fact_price_window_5m
) ranked
WHERE row_num = 1;

CREATE OR REPLACE VIEW vw_streaming_window_analytics AS

WITH streaming_data AS (
    SELECT
        f.coin_id,
        d.symbol,
        d.name,
        d.sector,
        f.window_start,
        f.window_end,
        f.avg_price_usd,
        f.min_price_usd,
        f.max_price_usd,
        f.event_count,

        f.max_price_usd - f.min_price_usd AS window_price_range,

        LAG(f.avg_price_usd) OVER (
            PARTITION BY f.coin_id
            ORDER BY f.window_end
        ) AS previous_window_avg_price,

        LAG(f.window_end) OVER (
            PARTITION BY f.coin_id
            ORDER BY f.window_end
        ) AS previous_window_end

    FROM fact_price_window_5m f
    LEFT JOIN dim_coin d
        ON f.coin_id = d.coin_id
)

SELECT
    coin_id,
    symbol,
    name,
    sector,
    window_start,
    window_end,
    avg_price_usd,
    min_price_usd,
    max_price_usd,
    event_count,
    window_price_range,
    previous_window_avg_price,

    CASE
        WHEN previous_window_avg_price IS NOT NULL
             AND previous_window_avg_price <> 0
             AND window_start - previous_window_end <= INTERVAL '10 minutes'
        THEN
            ((avg_price_usd - previous_window_avg_price)
             / previous_window_avg_price) * 100
    END AS window_price_change_pct,

    previous_window_end

FROM streaming_data;