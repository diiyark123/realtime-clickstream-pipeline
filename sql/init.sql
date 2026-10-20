-- Runs automatically the first time the Postgres container starts.

CREATE TABLE IF NOT EXISTS product_metrics_1m (
    window_start     TIMESTAMPTZ   NOT NULL,
    window_end       TIMESTAMPTZ   NOT NULL,
    product_id       TEXT          NOT NULL,
    category         TEXT,
    views            INTEGER       NOT NULL DEFAULT 0,
    add_to_carts     INTEGER       NOT NULL DEFAULT 0,
    purchases        INTEGER       NOT NULL DEFAULT 0,
    units_sold       INTEGER       NOT NULL DEFAULT 0,
    revenue          NUMERIC(12,2) NOT NULL DEFAULT 0,
    unique_users     INTEGER       NOT NULL DEFAULT 0,
    conversion_rate  NUMERIC(6,4),
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, product_id)
);

CREATE INDEX IF NOT EXISTS idx_metrics_category_window
    ON product_metrics_1m (category, window_start DESC);

CREATE TABLE IF NOT EXISTS quarantine_events (
    id               BIGSERIAL PRIMARY KEY,
    kafka_partition  INTEGER     NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    kafka_ts         TIMESTAMPTZ,
    raw_value        TEXT,
    error_reason     TEXT        NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kafka_partition, kafka_offset)
);

-- Convenience views for dashboards / ad-hoc analysis
CREATE OR REPLACE VIEW v_category_last_hour AS
SELECT category,
       SUM(views)        AS views,
       SUM(add_to_carts) AS add_to_carts,
       SUM(purchases)    AS purchases,
       SUM(revenue)      AS revenue,
       ROUND(SUM(purchases)::numeric / NULLIF(SUM(views), 0), 4) AS conversion_rate
FROM product_metrics_1m
WHERE window_start >= now() - INTERVAL '1 hour'
GROUP BY category;

CREATE OR REPLACE VIEW v_revenue_per_minute AS
SELECT window_start, SUM(revenue) AS revenue, SUM(purchases) AS purchases
FROM product_metrics_1m
GROUP BY window_start;
