import json
# pyrefly: ignore [missing-import]
from confluent_kafka import Consumer

TOPIC = "chat-messages"


def build_consumer(servers: str = "localhost:9092",
                   group: str = "moderation-workers") -> Consumer:
    """Create a Kafka consumer that starts from the earliest message."""
    return Consumer({
        "bootstrap.servers": servers,
        "group.id": group,
        "auto.offset.reset": "earliest",
    })


def parse(raw: bytes) -> dict:
    """Reverse the producer: bytes -> JSON string -> dict."""
    return json.loads(raw.decode("utf-8"))