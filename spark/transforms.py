"""Transformation logic, kept free of I/O so it can be tested on batch DataFrames."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("device", StringType()),
    StructField("country", StringType()),
    StructField("event_ts", StringType()),
])

REQUIRED = ["event_id", "user_id", "session_id", "event_type", "product_id"]
VALID_EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]


def parse_events(raw: DataFrame) -> DataFrame:
    """Parse Kafka rows into typed columns plus an `error_reason` (null = valid).

    Expects columns: value (string), partition, offset, timestamp (Kafka metadata).
    """
    parsed = raw.select(
        F.col("value").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.from_json(F.col("value"), EVENT_SCHEMA).alias("e"),
    ).select("raw_value", "kafka_partition", "kafka_offset", "kafka_ts", "e.*")

    parsed = parsed.withColumn("event_ts", F.to_timestamp("event_ts"))

    missing = [F.when(F.col(c).isNull(), F.lit(c)) for c in REQUIRED]
    error = (
        F.when(F.col("event_id").isNull() & F.col("user_id").isNull(), F.lit("unparseable_json"))
        .when(F.concat_ws(",", *missing) != "", F.concat(F.lit("missing:"), F.concat_ws(",", *missing)))
        .when(F.col("event_ts").isNull(), F.lit("invalid_event_ts"))
        .when(~F.col("event_type").isin(VALID_EVENT_TYPES), F.lit("unknown_event_type"))
        .when(F.coalesce(F.col("price"), F.lit(0.0)) < 0, F.lit("negative_price"))
    )
    return parsed.withColumn("error_reason", error)


def valid_events(parsed: DataFrame, watermark: str) -> DataFrame:
    """Valid rows, watermarked on event time and de-duplicated on event_id."""
    events = (
        parsed.where(F.col("error_reason").isNull())
        .drop("error_reason", "raw_value")
        .withColumn("quantity", F.coalesce("quantity", F.lit(0)))
        .withColumn("event_date", F.to_date("event_ts"))
        .withWatermark("event_ts", watermark)
    )
    # dropDuplicatesWithinWatermark is streaming-only; batch DataFrames (tests, backfills) use dropDuplicates.
    if events.isStreaming:
        return events.dropDuplicatesWithinWatermark(["event_id"])
    return events.dropDuplicates(["event_id"])


def invalid_events(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.col("error_reason").isNotNull()).select(
        "kafka_partition", "kafka_offset", "kafka_ts", "raw_value", "error_reason"
    )


def product_metrics(events: DataFrame, window: str) -> DataFrame:
    """Per-product funnel metrics in tumbling event-time windows."""
    is_type = lambda t: F.col("event_type") == t  # noqa: E731
    agg = (
        events.groupBy(F.window("event_ts", window).alias("w"), "product_id", "category")
        .agg(
            F.sum(is_type("page_view").cast("int")).alias("views"),
            F.sum(is_type("add_to_cart").cast("int")).alias("add_to_carts"),
            F.sum(is_type("purchase").cast("int")).alias("purchases"),
            F.sum(F.when(is_type("purchase"), F.col("quantity")).otherwise(0)).alias("units_sold"),
            F.sum(F.when(is_type("purchase"), F.col("price") * F.col("quantity")).otherwise(0.0)).alias("revenue"),
            F.approx_count_distinct("user_id").alias("unique_users"),
        )
    )
    return agg.select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "product_id",
        "category",
        "views",
        "add_to_carts",
        "purchases",
        "units_sold",
        F.round("revenue", 2).alias("revenue"),
        "unique_users",
        F.round(F.when(F.col("views") > 0, F.col("purchases") / F.col("views")), 4).alias("conversion_rate"),
    )
