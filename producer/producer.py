"""Publishes simulated clickstream events to Kafka at a steady rate."""

from __future__ import annotations

import logging
import os
import signal
import time

from confluent_kafka import Producer
from event_generator import ClickstreamGenerator, GeneratorConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "clickstream_events")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "50"))
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "0"))  # 0 = run forever
LOG_EVERY = int(os.getenv("LOG_EVERY", "1000"))

_running = True


def _stop(*_):
    global _running
    _running = False


def _on_delivery(err, msg):
    if err is not None:
        log.error("Delivery failed for key=%s: %s", msg.key(), err)


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,   # no duplicates from producer retries
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
    })
    gen = ClickstreamGenerator(GeneratorConfig())
    interval = 1.0 / EVENTS_PER_SECOND
    sent = 0
    started = time.time()
    log.info("Producing to %s/%s at %.0f events/s", BOOTSTRAP, TOPIC, EVENTS_PER_SECOND)

    while _running and (MAX_EVENTS == 0 or sent < MAX_EVENTS):
        key, value = gen.next_message()
        producer.produce(TOPIC, key=key.encode(), value=value.encode(), on_delivery=_on_delivery)
        producer.poll(0)
        sent += 1
        if sent % LOG_EVERY == 0:
            rate = sent / (time.time() - started)
            log.info("sent=%d rate=%.1f/s stats=%s", sent, rate, gen.stats)
        time.sleep(interval)

    log.info("Flushing %d messages...", len(producer))
    producer.flush(10)
    log.info("Done. sent=%d stats=%s", sent, gen.stats)


if __name__ == "__main__":
    main()
