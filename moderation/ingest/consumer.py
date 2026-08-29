import json

# pyrefly: ignore [missing-import]
from confluent_kafka import Consumer

from moderation.config import settings


def build_consumer(servers: str = None, group: str = None) -> Consumer:
    """Create a Kafka consumer that starts from the earliest message."""
    return Consumer({
        "bootstrap.servers": servers or settings.kafka_servers,
        "group.id": group or settings.consumer_group,
        "auto.offset.reset": "earliest",
        # We commit offsets ourselves, only after a batch is fully handled.
        "enable.auto.commit": False,
        # Notice a dead worker in ten seconds rather than the default
        # forty-five, so its partitions get picked up again quickly.
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
    })


def parse(raw: bytes) -> dict:
    """Reverse the producer: bytes -> JSON string -> dict."""
    return json.loads(raw.decode("utf-8"))
