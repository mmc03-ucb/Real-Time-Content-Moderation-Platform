import json

# pyrefly: ignore [missing-import] for VScode
from confluent_kafka import Producer

from moderation.config import settings


def build_producer(servers: str = None) -> Producer:
    """Create a Kafka producer connected to the given broker."""
    return Producer({
        "bootstrap.servers": servers or settings.kafka_servers,
        # Wait a few ms so small messages travel in batches instead of one by one.
        "linger.ms": 5,
        "compression.type": "lz4",
    })


def publish(producer: Producer, message: dict, topic: str = None) -> None:
    """
    Queue a message for sending. Does not block.

    We key on stream_id so every message from one stream lands on the same
    partition, which keeps that stream's messages in order.
    """
    payload = json.dumps(message).encode("utf-8")
    producer.produce(
        topic or settings.chat_topic,
        key=str(message.get("stream_id", "")).encode("utf-8"),
        value=payload,
    )
    # Give the background delivery thread a chance to run.
    producer.poll(0)


def flush(producer: Producer, timeout: float = 10.0) -> int:
    """Block until everything queued has been sent. Returns messages still pending."""
    return producer.flush(timeout)
