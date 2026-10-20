import json
from datetime import datetime, timedelta, timezone

from event_generator import PRODUCTS, REQUIRED_FIELDS, ClickstreamGenerator, GeneratorConfig

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def _messages(n=5000, **cfg):
    gen = ClickstreamGenerator(GeneratorConfig(seed=42, **cfg))
    return gen, [gen.next_message(NOW) for _ in range(n)]


def test_valid_events_have_required_fields_and_known_products():
    _, msgs = _messages(malformed_rate=0, duplicate_rate=0)
    product_ids = {p[0] for p in PRODUCTS}
    for _, value in msgs:
        e = json.loads(value)
        assert all(e.get(f) for f in REQUIRED_FIELDS)
        assert e["event_type"] in {"page_view", "add_to_cart", "purchase"}
        assert e["product_id"] in product_ids
        assert e["price"] > 0


def test_funnel_shape_views_gt_carts_gt_purchases():
    _, msgs = _messages(malformed_rate=0, duplicate_rate=0)
    counts = {}
    for _, v in msgs:
        t = json.loads(v)["event_type"]
        counts[t] = counts.get(t, 0) + 1
    assert counts["page_view"] > counts["add_to_cart"] > counts["purchase"] > 0


def test_purchases_have_positive_quantity_and_views_zero():
    _, msgs = _messages(malformed_rate=0, duplicate_rate=0)
    for _, v in msgs:
        e = json.loads(v)
        if e["event_type"] == "page_view":
            assert e["quantity"] == 0
        else:
            assert e["quantity"] >= 1


def test_duplicates_repeat_previous_event_id():
    gen, msgs = _messages(malformed_rate=0, duplicate_rate=0.05)
    ids = [json.loads(v)["event_id"] for _, v in msgs]
    assert gen.stats["duplicate"] > 0
    assert len(ids) - len(set(ids)) == gen.stats["duplicate"]


def test_malformed_events_are_actually_invalid():
    gen, msgs = _messages(malformed_rate=0.2, duplicate_rate=0)
    bad = [v for k, v in msgs if k == "malformed"]
    assert len(bad) == gen.stats["malformed"] > 0
    for v in bad:
        try:
            e = json.loads(v)
        except json.JSONDecodeError:
            continue
        missing = any(f not in e for f in REQUIRED_FIELDS)
        assert missing or e["event_ts"] == "not-a-timestamp"


def test_late_events_are_shifted_into_the_past():
    gen, msgs = _messages(malformed_rate=0, duplicate_rate=0, late_event_rate=0.2, very_late_rate=0.01)
    lags = [NOW - datetime.fromisoformat(json.loads(v)["event_ts"]) for _, v in msgs]
    late = [lag for lag in lags if lag > timedelta(0)]
    assert len(late) == gen.stats["late"] + gen.stats["very_late"]
    assert max(late) == timedelta(minutes=10)


def test_seed_makes_output_reproducible():
    _, a = _messages(n=200)
    _, b = _messages(n=200)
    assert a == b
