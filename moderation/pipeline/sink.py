"""
Where a verdict goes once it has been made.

Three places: MySQL for the audit trail, the decisions topic so anything
downstream can react, and the review queue when a human needs to look.
"""

import json
import logging

from moderation.config import settings
from moderation.obs import metrics
from moderation.pipeline.decisions import ESCALATE
from moderation.storage import dao

log = logging.getLogger(__name__)


class DecisionSink:
    """Writes one verdict everywhere it needs to go."""

    def __init__(self, conn, producer, publish):
        self.conn = conn
        self.producer = producer
        self._publish = publish
        self.written = 0
        self.duplicates = 0
        self.escalated = 0

    def handle(self, decision, message: dict) -> None:
        # INSERT IGNORE on msg_id: if Kafka replays a message after a crash we
        # record it once, so the counts stay right.
        is_new = dao.record_decision(self.conn, decision)
        if not is_new:
            self.duplicates += 1
            metrics.REPLAYS.inc()
            return
        self.written += 1

        self._publish(self.producer, decision.as_event(), settings.decisions_topic)

        if decision.action == ESCALATE:
            dao.enqueue_review(self.conn, decision, message["text"], decision.rule_hits)
            self._publish(self.producer, decision.as_event(), settings.review_topic)
            self.escalated += 1

        if decision.is_violation:
            dao.bump_user_risk(self.conn, decision.user_id, 1.0)

    def dead_letter(self, raw: bytes, error: str) -> None:
        """Park a message we could not even read, so the worker never stalls on it."""
        log.warning("could not handle a message: %s", error)
        metrics.DEAD_LETTERS.inc()
        self._publish(self.producer,
                      {"error": error, "raw": raw.decode("utf-8", "replace")},
                      settings.dead_letter_topic)
