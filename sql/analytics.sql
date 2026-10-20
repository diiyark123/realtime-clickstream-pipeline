-- Example queries. Run with: make psql   then   \i /sql/analytics.sql

-- 1. Revenue and orders per minute, most recent first
SELECT * FROM v_revenue_per_minute ORDER BY window_start DESC LIMIT 15;

-- 2. Category funnel for the last hour
SELECT * FROM v_category_last_hour ORDER BY revenue DESC;

-- 3. Top 5 products by revenue in the last 30 minutes
SELECT product_id, category, SUM(revenue) AS revenue, SUM(units_sold) AS units
FROM product_metrics_1m
WHERE window_start >= now() - INTERVAL '30 minutes'
GROUP BY product_id, category
ORDER BY revenue DESC
LIMIT 5;

-- 4. Data quality: quarantined events by reason
SELECT error_reason, COUNT(*) AS events, MAX(ingested_at) AS last_seen
FROM quarantine_events
GROUP BY error_reason
ORDER BY events DESC;

-- 5. Pipeline freshness: how far behind real time are the metrics?
SELECT now() - MAX(window_end) AS metrics_lag FROM product_metrics_1m;
