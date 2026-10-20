"""Simulated e-commerce clickstream.

Generates realistic user sessions (view -> add_to_cart -> purchase) and, on
purpose, a small share of "dirty" data so the streaming job has something to
handle:

* late events      - event_ts shifted into the past (out-of-order arrival)
* duplicate events - the same event_id sent twice (at-least-once delivery)
* malformed events - broken JSON or missing required fields

This module has no third-party dependencies so it can be unit tested anywhere.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

CATALOG = {
    "electronics": [("P1001", 899.0), ("P1002", 249.0), ("P1003", 59.0), ("P1004", 1299.0)],
    "fashion": [("P2001", 39.0), ("P2002", 79.0), ("P2003", 19.0)],
    "home": [("P3001", 149.0), ("P3002", 29.0), ("P3003", 499.0)],
    "books": [("P4001", 15.0), ("P4002", 22.0)],
    "sports": [("P5001", 89.0), ("P5002", 35.0)],
}
PRODUCTS = [(pid, cat, price) for cat, items in CATALOG.items() for pid, price in items]
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.6, 0.32, 0.08]
COUNTRIES = ["IN", "US", "GB", "SG", "AE", "DE"]
COUNTRY_WEIGHTS = [0.45, 0.2, 0.1, 0.1, 0.08, 0.07]

REQUIRED_FIELDS = ("event_id", "user_id", "session_id", "event_type", "product_id", "event_ts")


@dataclass
class GeneratorConfig:
    late_event_rate: float = 0.03       # share of events delivered late
    max_late_seconds: int = 90          # within the watermark -> still counted
    very_late_rate: float = 0.002       # beyond the watermark -> dropped by Spark
    duplicate_rate: float = 0.01
    malformed_rate: float = 0.005
    add_to_cart_prob: float = 0.25
    purchase_prob: float = 0.35         # of carted items
    session_end_prob: float = 0.08
    active_users: int = 200
    seed: int | None = None


@dataclass
class _Session:
    user_id: str
    session_id: str
    device: str
    country: str
    cart: list = field(default_factory=list)


class ClickstreamGenerator:
    def __init__(self, config: GeneratorConfig | None = None):
        self.cfg = config or GeneratorConfig()
        self.rng = random.Random(self.cfg.seed)
        self._sessions: list[_Session] = []
        self._last_event: dict | None = None
        self.stats = {"valid": 0, "late": 0, "very_late": 0, "duplicate": 0, "malformed": 0}

    # ---------- sessions ----------
    def _new_session(self) -> _Session:
        return _Session(
            user_id=f"U{self.rng.randint(1, 50_000):05d}",
            session_id=str(uuid.UUID(int=self.rng.getrandbits(128))),
            device=self.rng.choices(DEVICES, DEVICE_WEIGHTS)[0],
            country=self.rng.choices(COUNTRIES, COUNTRY_WEIGHTS)[0],
        )

    def _pick_session(self) -> _Session:
        if len(self._sessions) < self.cfg.active_users:
            self._sessions.append(self._new_session())
        session = self.rng.choice(self._sessions)
        if self.rng.random() < self.cfg.session_end_prob:
            self._sessions.remove(session)
        return session

    # ---------- events ----------
    def _base_event(self, s: _Session, event_type: str, product: tuple, qty: int, now: datetime) -> dict:
        pid, cat, price = product
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "user_id": s.user_id,
            "session_id": s.session_id,
            "event_type": event_type,
            "product_id": pid,
            "category": cat,
            "price": price,
            "quantity": qty,
            "device": s.device,
            "country": s.country,
            "event_ts": now.isoformat(timespec="milliseconds"),
        }

    def _valid_event(self, now: datetime) -> dict:
        s = self._pick_session()
        if s.cart and self.rng.random() < self.cfg.purchase_prob:
            product, qty = s.cart.pop(0)
            return self._base_event(s, "purchase", product, qty, now)
        product = self.rng.choice(PRODUCTS)
        if self.rng.random() < self.cfg.add_to_cart_prob:
            qty = self.rng.choices([1, 2, 3], [0.8, 0.15, 0.05])[0]
            s.cart.append((product, qty))
            return self._base_event(s, "add_to_cart", product, qty, now)
        return self._base_event(s, "page_view", product, 0, now)

    def _malformed(self, now: datetime) -> str:
        kind = self.rng.choice(["broken_json", "missing_field", "bad_type"])
        if kind == "broken_json":
            return '{"event_id": "' + str(uuid.uuid4()) + '", "user_id": '
        event = self._valid_event(now)
        if kind == "missing_field":
            event.pop(self.rng.choice(REQUIRED_FIELDS))
        else:
            event["event_ts"] = "not-a-timestamp"
        return json.dumps(event)

    def next_message(self, now: datetime | None = None) -> tuple[str, str]:
        """Return (key, value) for the next Kafka message."""
        now = now or datetime.now(timezone.utc)
        r = self.rng.random()

        if r < self.cfg.malformed_rate:
            self.stats["malformed"] += 1
            return "malformed", self._malformed(now)

        if r < self.cfg.malformed_rate + self.cfg.duplicate_rate and self._last_event:
            self.stats["duplicate"] += 1
            return self._last_event["user_id"], json.dumps(self._last_event)

        event = self._valid_event(now)
        late_roll = self.rng.random()
        if late_roll < self.cfg.very_late_rate:
            self.stats["very_late"] += 1
            event["event_ts"] = (now - timedelta(minutes=10)).isoformat(timespec="milliseconds")
        elif late_roll < self.cfg.very_late_rate + self.cfg.late_event_rate:
            self.stats["late"] += 1
            delay = self.rng.randint(5, self.cfg.max_late_seconds)
            event["event_ts"] = (now - timedelta(seconds=delay)).isoformat(timespec="milliseconds")
        else:
            self.stats["valid"] += 1

        self._last_event = event
        return event["user_id"], json.dumps(event)
