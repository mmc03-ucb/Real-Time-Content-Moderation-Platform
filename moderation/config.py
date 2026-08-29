"""
All the knobs StreamGuard reads from the environment.

Everything has a sensible default so `docker compose up` works with no setup.
"""

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


@dataclass(frozen=True)
class Settings:
    """One place to look when you want to know how a process is configured."""

    # Kafka
    kafka_servers: str = os.getenv("KAFKA_SERVERS", "localhost:9092")
    chat_topic: str = os.getenv("CHAT_TOPIC", "chat.events")
    decisions_topic: str = os.getenv("DECISIONS_TOPIC", "moderation.decisions")
    review_topic: str = os.getenv("REVIEW_TOPIC", "review.queue")
    dead_letter_topic: str = os.getenv("DEAD_LETTER_TOPIC", "chat.deadletter")
    consumer_group: str = os.getenv("CONSUMER_GROUP", "moderation-workers")

    # Redis
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # MySQL
    mysql_host: str = os.getenv("MYSQL_HOST", "localhost")
    mysql_port: int = _int("MYSQL_PORT", 3306)
    mysql_user: str = os.getenv("MYSQL_USER", "streamguard")
    mysql_password: str = os.getenv("MYSQL_PASSWORD", "streamguard")
    mysql_database: str = os.getenv("MYSQL_DATABASE", "streamguard")

    # How many messages a worker grabs per poll, and how long it waits for them.
    # Bigger batches mean fewer, larger calls to the toxicity model.
    batch_size: int = _int("BATCH_SIZE", 500)
    batch_wait_seconds: float = _float("BATCH_WAIT_SECONDS", 0.1)

    # How often workers check Redis to see if the rules changed.
    rules_poll_seconds: float = _float("RULES_POLL_SECONDS", 3.0)

    # Where each worker exposes its Prometheus metrics.
    metrics_port: int = _int("METRICS_PORT", 9100)


settings = Settings()
