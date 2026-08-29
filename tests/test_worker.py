import pytest

from moderation.config import settings
from moderation.ingest.generator import make_message
from moderation.ml.classifier import SafeClassifier
from moderation.pipeline import decisions as d
from moderation.pipeline import sink as sink_module
from moderation.pipeline.pipeline import ModerationPipeline
from moderation.pipeline.sink import DecisionSink
from moderation.pipeline.worker import Worker
from moderation.rules.engine import RuleEngine
from tests.fakes import (FakeConsumer, FakeDao, FakeKafkaMessage,
                         RecordingPublish, encode)


class StaticStore:
    def current(self):
        return RuleEngine([])


class FixedScores:
    name = "fixed"

    def __init__(self, score):
        self.score = score

    def score_batch(self, texts):
        return [self.score] * len(texts)


@pytest.fixture
def parts(fake_redis, monkeypatch):
    """A worker wired to fakes, plus the pieces a test wants to look at."""
    def _build(batches, score=0.0):
        dao = FakeDao()
        monkeypatch.setattr(sink_module, "dao", dao)
        publish = RecordingPublish()
        pipeline = ModerationPipeline(StaticStore(), SafeClassifier(FixedScores(score)),
                                      fake_redis)
        consumer = FakeConsumer(batches)
        worker = Worker(consumer, pipeline, DecisionSink(None, None, publish))
        return worker, dao, publish, consumer
    return _build


def chat(text="hello", user="u1"):
    return make_message(user, text, stream_id="s1", client_meta={"account_age_days": 400})


def test_a_batch_is_moderated_and_recorded(parts):
    batch = [encode(chat(f"line {i}", user=f"u{i}")) for i in range(5)]
    worker, dao, publish, _ = parts([batch])
    assert worker.run_once() == 5
    assert len(dao.decisions) == 5
    assert len(publish.on(settings.decisions_topic)) == 5


def test_offsets_are_committed_only_after_the_batch_is_written(parts):
    worker, dao, _, consumer = parts([[encode(chat())]])
    worker.run_once()
    assert consumer.commits == 1
    assert len(dao.decisions) == 1


def test_a_replayed_message_is_only_recorded_once(parts):
    message = chat()
    worker, dao, publish, _ = parts([[encode(message)], [encode(message)]])
    worker.run_once()
    worker.run_once()
    assert len(dao.decisions) == 1
    assert len(publish.on(settings.decisions_topic)) == 1


def test_unreadable_messages_go_to_the_dead_letter_topic(parts):
    worker, dao, publish, _ = parts([[FakeKafkaMessage(b"not json at all")]])
    worker.run_once()
    assert len(publish.on(settings.dead_letter_topic)) == 1
    assert dao.decisions == {}


def test_a_message_missing_fields_is_dead_lettered(parts):
    worker, _, publish, _ = parts([[encode({"msg_id": "1", "text": "hi"})]])
    worker.run_once()
    assert len(publish.on(settings.dead_letter_topic)) == 1


def test_one_bad_message_does_not_stop_the_rest_of_the_batch(parts):
    batch = [FakeKafkaMessage(b"broken"), encode(chat("fine", user="u2"))]
    worker, dao, _, _ = parts([batch])
    assert worker.run_once() == 1
    assert len(dao.decisions) == 1


def test_escalated_messages_reach_the_review_queue(parts):
    worker, dao, publish, _ = parts([[encode(chat())]], score=0.7)
    worker.run_once()
    assert len(dao.reviews) == 1
    assert dao.reviews[0]["text"] == "hello"
    assert len(publish.on(settings.review_topic)) == 1


def test_violations_count_against_the_user(parts):
    worker, dao, _, _ = parts([[encode(chat())]], score=0.99)
    worker.run_once()
    assert dao.risk_bumps == [("u1", 1.0)]


def test_an_empty_poll_does_nothing(parts):
    worker, dao, publish, consumer = parts([])
    assert worker.run_once() == 0
    assert publish.sent == []
