"""Kafka -> Spark Structured Streaming -> Parquet lake + PostgreSQL.

Three streaming queries run side by side, each with its own checkpoint:

1. events_lake       cleaned, de-duplicated events -> Parquet (partitioned by date)
2. product_metrics   1-minute funnel metrics per product -> Postgres (idempotent upsert)
3. quarantine        malformed events -> Postgres, for inspection and replay
"""

from __future__ import annotations

import logging
import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQueryListener
from transforms import invalid_events, parse_events, product_metrics, valid_events

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("py4j").setLevel(logging.WARNING)  # hide per-callback "Received command" noise
log = logging.getLogger("streaming_job")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "clickstream_events")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")
WATERMARK = os.getenv("WATERMARK_DELAY", "2 minutes")
WINDOW = os.getenv("METRICS_WINDOW", "1 minute")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "10 seconds")
LAKE_DIR = os.getenv("LAKE_DIR", "/data/lake/events")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/data/checkpoints")

PG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "clickstream"),
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline"),
}

METRIC_COLS = [
    "window_start", "window_end", "product_id", "category", "views", "add_to_carts",
    "purchases", "units_sold", "revenue", "unique_users", "conversion_rate",
]

UPSERT_METRICS = f"""
INSERT INTO product_metrics_1m ({", ".join(METRIC_COLS)})
VALUES %s
ON CONFLICT (window_start, product_id) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    category = EXCLUDED.category,
    views = EXCLUDED.views,
    add_to_carts = EXCLUDED.add_to_carts,
    purchases = EXCLUDED.purchases,
    units_sold = EXCLUDED.units_sold,
    revenue = EXCLUDED.revenue,
    unique_users = EXCLUDED.unique_users,
    conversion_rate = EXCLUDED.conversion_rate,
    updated_at = now()
"""

INSERT_QUARANTINE = """
INSERT INTO quarantine_events (kafka_partition, kafka_offset, kafka_ts, raw_value, error_reason)
VALUES %s
ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
"""


def _write_rows(sql: str, rows: list[tuple], batch_id: int, name: str) -> None:
    if not rows:
        return
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    log.info("[%s] batch=%d wrote %d rows", name, batch_id, len(rows))


def write_metrics(batch: DataFrame, batch_id: int) -> None:
    # Aggregated output is small (products x windows), so collecting to the driver is fine.
    rows = [tuple(r[c] for c in METRIC_COLS) for r in batch.select(*METRIC_COLS).collect()]
    _write_rows(UPSERT_METRICS, rows, batch_id, "product_metrics")


def write_quarantine(batch: DataFrame, batch_id: int) -> None:
    cols = ["kafka_partition", "kafka_offset", "kafka_ts", "raw_value", "error_reason"]
    rows = [tuple(r[c] for c in cols) for r in batch.select(*cols).collect()]
    _write_rows(INSERT_QUARANTINE, rows, batch_id, "quarantine")


class ProgressLogger(StreamingQueryListener):
    """One readable log line per query per micro-batch: rows read, watermark, rows dropped as too late."""

    def onQueryStarted(self, event):
        log.info("[%s] started", event.name)

    def onQueryProgress(self, event):
        p = event.progress
        if p.numInputRows == 0:
            return
        dropped = sum(op.numRowsDroppedByWatermark for op in p.stateOperators)
        watermark = (p.eventTime or {}).get("watermark", "-")
        log.info("[%s] batch=%d read=%d rows watermark=%s dropped_too_late=%d",
                 p.name, p.batchId, p.numInputRows, watermark, dropped)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.warning("query %s terminated: %s", event.id, event.exception)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("clickstream-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS_PER_TRIGGER", "20000"))
        .load()
        .selectExpr("CAST(value AS STRING) AS value", "partition", "offset", "timestamp")
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger())
    log.info("Reading %s from %s | watermark=%s window=%s", TOPIC, KAFKA_BOOTSTRAP, WATERMARK, WINDOW)

    parsed = parse_events(read_kafka(spark))
    events = valid_events(parsed, WATERMARK)

    (
        events.writeStream.queryName("events_lake")
        .format("parquet")
        .option("path", LAKE_DIR)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/events_lake")
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    # Append mode: each window is emitted once, after the watermark passes its end,
    # so the numbers written are final. The upsert keeps restarts idempotent.
    (
        product_metrics(events, WINDOW).writeStream.queryName("product_metrics")
        .foreachBatch(write_metrics)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/product_metrics")
        .outputMode("append")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    (
        invalid_events(parsed).writeStream.queryName("quarantine")
        .foreachBatch(write_quarantine)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/quarantine")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
