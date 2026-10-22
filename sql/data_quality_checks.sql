/* Duplicate event check */
SELECT event_id, COUNT(*) AS duplicate_count FROM fact_price_tick
GROUP BY event_id
HAVING COUNT(*) > 1;

/*Invalid price check */
SELECT event_id, coin_id, event_time, price_usd FROM fact_price_tick
WHERE price_usd IS NULL OR price_usd <= 0;

/*Data freshness check */
SELECT
  MAX(event_time) AS latest_event_time,
  CURRENT_TIMESTAMP AS current_time,
  CURRENT_TIMESTAMP - MAX(event_time) AS data_age
FROM fact_price_tick;

/*Recent coin coverage*/
SELECT
    COUNT(DISTINCT coin_id) AS recent_coin_count
FROM fact_price_tick
WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '15 minutes';

/* Event volume by coin */
SELECT
    coin_id,
    COUNT(*) AS events_last_hour,
    MAX(event_time) AS latest_event_time
FROM fact_price_tick
WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
GROUP BY coin_id
ORDER BY events_last_hour DESC;