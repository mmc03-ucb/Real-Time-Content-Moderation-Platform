"""
Where verdicts go once they have been made.

Three places: MySQL for the audit trail, the decisions topic so anything
downstream can react, and the review queue when a human needs to look. Written
a batch at a time, because one round trip for sixty rows beats sixty.
"""

import logging

from moderation.config import settings
from moderation.obs import metrics
from moderation.pipeline.decisions import ESCALATE
from moderation.storage import dao

log = logging.getLogger(__name__)


class DecisionSink:
    """Writes a batch of verdicts everywhere they need to go."""

    def __init__(self, conn, producer, publish):
        self.conn = conn
        self.producer = producer
        self._publish = publish
        self.written = 0
        self.replays = 0
        self.escalated = 0

    def handle_batch(self, decisions, messages_by_id: dict) -> None:
        if not decisions:
            return

        with metrics.STAGE_SECONDS.labels("mysql").time():
            fresh = self._save(decisions, messages_by_id)

        with metrics.STAGE_SECONDS.labels("kafka").time():
            for decision in fresh:
                self._publish(self.producer, decision.as_event(),
                              settings.decisions_topic)
                if decision.action == ESCALATE:
                    self._publish(self.producer, decision.as_event(),
                                  settings.review_topic)

    def _save(self, decisions, messages_by_id: dict):
        """Write the batch to MySQL and return the verdicts that were new."""
        # Anything already on record is a Kafka replay after a crash. It has
        # been handled once, so we leave it alone.
        already_decided = dao.existing_msg_ids(self.conn,
                                               [d.msg_id for d in decisions])
        fresh = [d for d in decisions if d.msg_id not in already_decided]
        if len(fresh) != len(decisions):
            self.replays += len(decisions) - len(fresh)
            metrics.REPLAYS.inc(len(decisions) - len(fresh))
        if not fresh:
            return []

        dao.record_decisions(self.conn, fresh)
        self.written += len(fresh)

        escalations = [d for d in fresh if d.action == ESCALATE]
        if escalations:
            dao.enqueue_reviews(self.conn, [(d, messages_by_id[d.msg_id]["text"])
                                            for d in escalations])
            self.escalated += len(escalations)

        dao.bump_user_risks(self.conn, [(d.user_id, 1.0) for d in fresh
                                        if d.is_violation])
        return fresh

    def dead_letter(self, raw: bytes, error: str) -> None:
        """Park a message we could not even read, so the worker never stalls on it."""
        log.warning("could not handle a message: %s", error)
        metrics.DEAD_LETTERS.inc()
        self._publish(self.producer,
                      {"error": error, "raw": raw.decode("utf-8", "replace")},
                      settings.dead_letter_topic)
