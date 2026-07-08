-- ============================================================
-- v_price_anomalies: detect abnormal price jumps per coin
-- ============================================================
CREATE VIEW v_price_anomalies AS
SELECT
    coin_id,
    price_date,
    price_usd,
    LAG(price_usd) OVER (PARTITION BY coin_id ORDER BY price_date) AS previous_price,
    ROUND(
        ((price_usd - LAG(price_usd) OVER (PARTITION BY coin_id ORDER BY price_date))
        / NULLIF(LAG(price_usd) OVER (PARTITION BY coin_id ORDER BY price_date), 0)) * 100,
        2
    ) AS pct_change
FROM fact_coin_prices
WHERE price_date >= CURRENT_DATE - INTERVAL '30 days';

-- ============================================================
-- v_missing_dates: detect missing dates (pipeline run failures)
-- ============================================================
CREATE VIEW v_missing_dates AS
SELECT
    d.coin_id,
    gs.expected_date
FROM dim_coins d
CROSS JOIN generate_series(
    (SELECT MIN(price_date) FROM fact_coin_prices),
    CURRENT_DATE,
    '1 day'
) AS gs(expected_date)
LEFT JOIN fact_coin_prices f
    ON f.coin_id = d.coin_id AND f.price_date = gs.expected_date::date
WHERE f.coin_id IS NULL
  AND d.coin_id IN (SELECT DISTINCT coin_id FROM fact_coin_prices);

-- ============================================================
-- v_pipeline_health: pipeline run status summary
-- ============================================================
CREATE VIEW v_pipeline_health AS
SELECT
    stage,
    status,
    COUNT(*) AS total_runs,
    MAX(run_date) AS last_run,
    SUM(rows_processed) AS total_rows_processed
FROM pipeline_run_log
GROUP BY stage, status
ORDER BY stage, status;
