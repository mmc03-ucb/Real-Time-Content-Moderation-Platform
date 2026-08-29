"""Stand-ins for Kafka and MySQL so the worker can be tested with nothing running."""

import json


class FakeKafkaMessage:
    def __init__(self, value: bytes, error=None):
        self._value = value
        self._error = error

    def value(self):
        return self._value

    def error(self):
        return self._error


class FakeConsumer:
    """Hands out prepared batches, then nothing."""

    def __init__(self, batches):
        self.batches = list(batches)
        self.commits = 0
        self.closed = False

    def consume(self, num_messages, timeout):
        return self.batches.pop(0) if self.batches else []

    def commit(self, asynchronous=False):
        self.commits += 1

    def close(self):
        self.closed = True


class RecordingPublish:
    """Captures what would have been sent to Kafka."""

    def __init__(self):
        self.sent = []

    def __call__(self, producer, message, topic):
        self.sent.append((topic, message))

    def on(self, topic):
        return [m for t, m in self.sent if t == topic]


class FakeDao:
    """In-memory version of the queries the sink runs."""

    def __init__(self):
        self.decisions = {}
        self.reviews = []
        self.risk_bumps = []

    def record_decision(self, conn, decision):
        if decision.msg_id in self.decisions:
            return False
        self.decisions[decision.msg_id] = decision
        return True

    def enqueue_review(self, conn, decision, text, rule_hits):
        self.reviews.append({"msg_id": decision.msg_id, "text": text,
                             "rule_hits": rule_hits})
        return True

    def bump_user_risk(self, conn, user_id, amount):
        self.risk_bumps.append((user_id, amount))


def encode(message: dict) -> FakeKafkaMessage:
    return FakeKafkaMessage(json.dumps(message).encode("utf-8"))
