"""
The moderation worker.

Run several of these against the same consumer group and Kafka splits the
partitions between them, which is how the platform scales out. Offsets are only
committed once a batch has been fully written, so a worker that dies mid-batch
means those messages are handled again rather than lost.

    python -m moderation.pipeline.worker
"""

import logging
import signal
import time

from moderation.config import settings
from moderation.defenses.client import build_redis
from moderation.ingest.consumer import build_consumer, parse
from moderation.ingest.producer import build_producer, flush, publish
from moderation.ml.classifier import load_classifier
from moderation.obs import metrics, tracing
from moderation.pipeline.pipeline import ModerationPipeline
from moderation.pipeline.sink import DecisionSink
from moderation.rules.store import RuleStore, read_version
from moderation.storage import dao, mysql
from moderation.strategies import ab

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("worker")

REQUIRED_FIELDS = ("msg_id", "stream_id", "user_id", "text", "ts")


class Worker:
    """Pulls batches off Kafka, moderates them, writes the results down."""

    def __init__(self, consumer, pipeline, sink, batch_size=None, batch_wait=None):
        self.consumer = consumer
        self.pipeline = pipeline
        self.sink = sink
        self.batch_size = batch_size or settings.batch_size
        self.batch_wait = batch_wait if batch_wait is not None else settings.batch_wait_seconds
        self.running = True
        self.handled = 0
        self._lag_checked_at = 0.0

    def stop(self, *_):
        log.info("shutting down after this batch")
        self.running = False

    def run(self) -> None:
        while self.running:
            try:
                self.run_once()
            except Exception:
                # Offsets were not committed, so Kafka will hand this batch back.
                # Better to retry a batch than to lose a worker to a blip.
                metrics.BATCH_FAILURES.inc()
                log.exception("batch failed, it will be handled again")
                time.sleep(0.5)

    def run_once(self) -> int:
        """
        Handle one batch.

        Kafka gives us up to batch_size messages, or whatever has arrived after
        batch_wait seconds. That is also what makes model calls efficient: one
        call for the whole batch instead of one per message.
        """
        raw_messages = self.consumer.consume(num_messages=self.batch_size,
                                             timeout=self.batch_wait)
        if not raw_messages:
            return 0

        messages = []
        for raw in raw_messages:
            if raw.error():
                log.warning("kafka error: %s", raw.error())
                continue
            message = self._read(raw)
            if message is not None:
                messages.append(message)

        if messages:
            decisions = self.pipeline.evaluate_batch(messages)
            self.sink.handle_batch(decisions, self._by_id(messages))
            self.handled += len(messages)

        # Only now do we tell Kafka we are done with these offsets. A worker
        # that dies before this point simply sees the batch again.
        with metrics.STAGE_SECONDS.labels("commit").time():
            self.consumer.commit(asynchronous=False)
        self._report_lag()
        return len(messages)

    def _report_lag(self) -> None:
        """Publish how far behind we are, at most once a second."""
        now = time.monotonic()
        if now - self._lag_checked_at < 1.0:
            return
        self._lag_checked_at = now
        try:
            metrics.CONSUMER_LAG.set(metrics.consumer_lag(self.consumer))
        except Exception:
            pass

    def _read(self, raw):
        """Parse a message, or send it to the dead letter topic if it is broken."""
        try:
            message = parse(raw.value())
            missing = [f for f in REQUIRED_FIELDS if f not in message]
            if missing:
                raise ValueError(f"missing fields: {', '.join(missing)}")
            return message
        except Exception as exc:
            self.sink.dead_letter(raw.value() or b"", str(exc))
            return None

    @staticmethod
    def _by_id(messages):
        return {m["msg_id"]: m for m in messages}


def build_worker():
    """Wire up every dependency the worker needs."""
    conn = mysql.connect()
    redis_client = build_redis()
    producer = build_producer()

    rule_store = RuleStore(
        load_rules=lambda: dao.load_rules(conn),
        read_version=lambda: read_version(redis_client),
        poll_seconds=settings.rules_poll_seconds,
    )
    pipeline = ModerationPipeline(
        rule_store=rule_store,
        classifier=load_classifier(),
        redis_client=redis_client,
        strategies=ab.from_rows(dao.load_strategies(conn)),
    )
    consumer = build_consumer()
    consumer.subscribe([settings.chat_topic])

    return Worker(consumer, pipeline, DecisionSink(conn, producer, publish)), producer


def main() -> None:
    tracing.setup()
    port = metrics.serve(settings.metrics_port)
    log.info("metrics on http://localhost:%d/metrics", port)

    worker, producer = build_worker()
    signal.signal(signal.SIGINT, worker.stop)
    signal.signal(signal.SIGTERM, worker.stop)

    log.info("worker ready, reading %s", settings.chat_topic)
    started = time.perf_counter()
    try:
        worker.run()
    finally:
        flush(producer)
        worker.consumer.close()
        took = time.perf_counter() - started
        log.info("handled %d messages in %.1fs", worker.handled, took)


if __name__ == "__main__":
    main()
