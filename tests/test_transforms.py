"""Spark transformation tests on static DataFrames. Skipped if PySpark isn't installed."""

import json
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from transforms import invalid_events, parse_events, product_metrics, valid_events  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


def ev(event_id, event_type="page_view", product="P1001", qty=0, price=10.0, ts="2026-01-01T12:00:10", user="U1"):
    return json.dumps({
        "event_id": event_id, "user_id": user, "session_id": "S1", "event_type": event_type,
        "product_id": product, "category": "electronics", "price": price, "quantity": qty,
        "device": "mobile", "country": "IN", "event_ts": ts,
    })


def kafka_df(spark, values):
    rows = [(v, 0, i, datetime(2026, 1, 1, 12)) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, "value string, partition int, offset long, timestamp timestamp")


def test_parse_flags_bad_records(spark):
    missing = json.loads(ev("e3"))
    del missing["user_id"]
    bad_ts = json.loads(ev("e4"))
    bad_ts["event_ts"] = "nope"
    values = [ev("e1"), '{"broken": ', json.dumps(missing), json.dumps(bad_ts), ev("e5", event_type="refund")]
    parsed = parse_events(kafka_df(spark, values))
    reasons = {r.kafka_offset: r.error_reason for r in parsed.collect()}
    assert reasons[0] is None
    assert reasons[1] == "unparseable_json"
    assert reasons[2] == "missing:user_id"
    assert reasons[3] == "invalid_event_ts"
    assert reasons[4] == "unknown_event_type"
    assert invalid_events(parsed).count() == 4


def test_duplicates_removed(spark):
    values = [ev("e1"), ev("e1"), ev("e2")]
    events = valid_events(parse_events(kafka_df(spark, values)), "2 minutes")
    assert sorted(r.event_id for r in events.collect()) == ["e1", "e2"]


def test_product_metrics(spark):
    values = [
        ev("v1"), ev("v2", user="U2"), ev("v3", user="U3"), ev("v4", user="U4"),
        ev("c1", "add_to_cart", qty=2),
        ev("p1", "purchase", qty=2, price=10.0),
        ev("v5", ts="2026-01-01T12:01:30"),  # next window
    ]
    events = valid_events(parse_events(kafka_df(spark, values)), "2 minutes")
    rows = {str(r.window_start): r for r in product_metrics(events, "1 minute").collect()}
    first = rows["2026-01-01 12:00:00"]
    assert (first.views, first.add_to_carts, first.purchases, first.units_sold) == (4, 1, 1, 2)
    assert first.revenue == 20.0
    assert first.conversion_rate == 0.25
    assert rows["2026-01-01 12:01:00"].views == 1
