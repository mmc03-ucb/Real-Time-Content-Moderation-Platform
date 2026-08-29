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

    def existing_msg_ids(self, conn, msg_ids):
        return {i for i in msg_ids if i in self.decisions}

    def record_decisions(self, conn, decisions):
        for decision in decisions:
            self.decisions[decision.msg_id] = decision
        return len(decisions)

    def enqueue_reviews(self, conn, items):
        self.reviews.extend({"msg_id": d.msg_id, "text": text,
                             "rule_hits": d.rule_hits} for d, text in items)
        return len(items)

    def bump_user_risks(self, conn, users):
        self.risk_bumps.extend(users)
        return len(users)


def encode(message: dict) -> FakeKafkaMessage:
    return FakeKafkaMessage(json.dumps(message).encode("utf-8"))
