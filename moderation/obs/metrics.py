"""
The numbers StreamGuard reports about itself.

Every worker exposes these on its own /metrics page. Prometheus scrapes them
and Grafana draws the dashboard, so "is it keeping up?" has an answer you can
point at rather than guess.
"""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

MESSAGES = Counter(
    "streamguard_messages_total", "Chat messages pulled off Kafka and moderated")

DECISIONS = Counter(
    "streamguard_decisions_total", "Verdicts made",
    ["action", "reason_code", "strategy"])

REPLAYS = Counter(
    "streamguard_replayed_messages_total",
    "Messages Kafka delivered again that we had already decided")

DEAD_LETTERS = Counter(
    "streamguard_dead_letters_total", "Messages we could not read at all")

# Split by stage so a slowdown can be traced to the part that caused it.
STAGE_SECONDS = Histogram(
    "streamguard_stage_seconds", "Time spent in one stage of the funnel",
    ["stage"], buckets=(.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5))

END_TO_END_MS = Histogram(
    "streamguard_end_to_end_ms", "Milliseconds from a viewer sending a message to a verdict",
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000))

BATCH_SIZE = Histogram(
    "streamguard_batch_size", "Messages handled per poll",
    buckets=(1, 2, 5, 10, 25, 50, 100, 250, 500))

CONSUMER_LAG = Gauge(
    "streamguard_consumer_lag", "Messages waiting on Kafka for this worker")

REVIEW_QUEUE_DEPTH = Gauge(
    "streamguard_review_queue_depth", "Messages waiting on a human")

MODEL_HEALTHY = Gauge(
    "streamguard_model_healthy", "1 when the toxicity model is answering, 0 when it is not")

RULES_VERSION = Gauge(
    "streamguard_rules_version", "Ruleset version this worker is running")


def serve(port: int, attempts: int = 10) -> int:
    """
    Open the /metrics page for Prometheus to scrape.

    Several workers usually run on one machine, so if the port is taken we take
    the next one up and report which we got.
    """
    for offset in range(attempts):
        try:
            start_http_server(port + offset)
            return port + offset
        except OSError:
            continue
    raise OSError(f"no free port for metrics between {port} and {port + attempts}")


def consumer_lag(consumer) -> int:
    """
    How far behind this worker is.

    Lag climbing steadily is the signal to start another worker; the whole
    point of the consumer group is that doing so needs no other change.
    """
    total = 0
    for partition in consumer.assignment():
        try:
            _, high = consumer.get_watermark_offsets(partition, cached=True)
            position = consumer.position([partition])[0].offset
        except Exception:
            continue
        if high is not None and position is not None and position >= 0:
            total += max(0, high - position)
    return total
