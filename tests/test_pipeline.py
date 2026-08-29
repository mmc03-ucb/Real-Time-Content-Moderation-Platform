import pytest

from moderation.ingest.generator import make_message
from moderation.ml.classifier import SafeClassifier
from moderation.pipeline import decisions as d
from moderation.pipeline.pipeline import ModerationPipeline
from moderation.rules.engine import RuleEngine
from moderation.rules.models import Rule
from moderation.strategies import ab


class FixedScores:
    """A stand-in model that returns whatever score the test asks for."""

    name = "fixed"

    def __init__(self, score=0.0):
        self.score = score
        self.batch_sizes = []

    def score_batch(self, texts):
        self.batch_sizes.append(len(texts))
        return [self.score] * len(texts)


class StaticStore:
    def __init__(self, rules=()):
        self._engine = RuleEngine(list(rules))

    def current(self):
        return self._engine


def keyword_rule(pattern="idiot", action="delete", rule_id=1):
    return Rule(id=rule_id, name="bad words", rule_type="keyword", pattern=pattern,
                threshold=None, action=action, priority=10, enabled=True,
                stream_id=None, version=1)


@pytest.fixture
def build(fake_redis):
    def _build(rules=(), score=0.0, strategies=None, raid_threshold=25):
        model = FixedScores(score)
        pipeline = ModerationPipeline(StaticStore(rules), SafeClassifier(model),
                                      fake_redis,
                                      strategies=strategies or [ab.Strategy("A")],
                                      raid_threshold=raid_threshold)
        return pipeline, model
    return _build


def chat(text="hello", user="u1", stream="s1", age=400):
    return make_message(user, text, stream_id=stream,
                        client_meta={"account_age_days": age})


def test_clean_chat_is_allowed(build):
    pipeline, _ = build()
    [decision] = pipeline.evaluate_batch([chat()])
    assert decision.action == d.ALLOW
    assert decision.reason_code == d.CLEAN


def test_a_rule_match_beats_the_model(build):
    pipeline, model = build(rules=[keyword_rule()], score=0.0)
    [decision] = pipeline.evaluate_batch([chat("you idiot")])
    assert decision.action == d.DELETE
    assert decision.reason_code == d.RULE_MATCH
    assert decision.rule_id == 1
    # The model was never asked about it, which is the point of rules first.
    assert model.batch_sizes == [0]


def test_a_confident_model_score_deletes(build):
    pipeline, _ = build(score=0.95)
    [decision] = pipeline.evaluate_batch([chat()])
    assert decision.action == d.DELETE
    assert decision.reason_code == d.ML_TOXIC


def test_an_unsure_model_score_goes_to_a_human(build):
    pipeline, _ = build(score=0.7)
    [decision] = pipeline.evaluate_batch([chat()])
    assert decision.action == d.ESCALATE
    assert decision.reason_code == d.ML_UNCERTAIN


def test_repeating_yourself_is_caught(build):
    pipeline, _ = build()
    pipeline.evaluate_batch([chat("buy my stuff")])
    [decision] = pipeline.evaluate_batch([chat("buy my stuff")])
    assert decision.reason_code == d.DUPLICATE


def test_sending_too_fast_is_caught(build):
    pipeline, _ = build()
    # Different text each time, so this is the speed limit and not the dedup check.
    for i in range(10):
        pipeline.evaluate_batch([chat(f"message {i}")])
    [decision] = pipeline.evaluate_batch([chat("message 11")])
    assert decision.reason_code == d.RATE_LIMIT


def test_new_accounts_are_hidden_during_a_raid(build):
    pipeline, _ = build(raid_threshold=5)
    raiders = [chat("hi there", user=f"raider_{i}", age=0) for i in range(6)]
    pipeline.evaluate_batch(raiders)
    [decision] = pipeline.evaluate_batch([chat("hello", user="raider_new", age=0)])
    assert decision.action == d.SHADOW
    assert decision.reason_code == d.RAID_MODE


def test_established_users_chat_on_through_a_raid(build):
    pipeline, _ = build(raid_threshold=5)
    pipeline.evaluate_batch([chat("hi", user=f"raider_{i}", age=0) for i in range(6)])
    [decision] = pipeline.evaluate_batch([chat("hello", user="regular", age=500)])
    assert decision.action == d.ALLOW


def test_the_model_sees_the_batch_once(build):
    pipeline, model = build()
    pipeline.evaluate_batch([chat(f"line {i}", user=f"u{i}") for i in range(20)])
    assert model.batch_sizes == [20]


def test_repeat_offenders_are_judged_more_strictly(build, fake_redis):
    from moderation.defenses import risk
    strict = [ab.Strategy("A", delete_threshold=0.9, escalate_threshold=0.6,
                          risk_bonus=0.3)]
    pipeline, _ = build(score=0.65, strategies=strict)

    [first] = pipeline.evaluate_batch([chat("borderline", user="repeat")])
    assert first.action == d.ESCALATE

    # After a pile of violations the same score now clears the delete line.
    risk.add_violation(fake_redis, "repeat", 10.0)
    [second] = pipeline.evaluate_batch([chat("borderline again", user="repeat")])
    assert second.action == d.DELETE


def test_a_stream_always_lands_in_the_same_bucket(build):
    pipeline, _ = build(strategies=list(ab.DEFAULT))
    names = {pipeline.evaluate_batch([chat(f"hi {i}", user=f"u{i}", stream="s7")])[0].strategy
             for i in range(5)}
    assert len(names) == 1


def test_messages_are_still_allowed_when_the_model_is_down(fake_redis):
    class Broken:
        name = "broken"

        def score_batch(self, texts):
            raise RuntimeError("down")

    pipeline = ModerationPipeline(StaticStore([]), SafeClassifier(Broken()), fake_redis)
    [decision] = pipeline.evaluate_batch([chat()])
    assert decision.action == d.ALLOW
    assert decision.reason_code == d.ML_UNAVAILABLE


def test_every_verdict_carries_a_latency(build):
    pipeline, _ = build()
    [decision] = pipeline.evaluate_batch([chat()])
    assert decision.latency_ms >= 0
