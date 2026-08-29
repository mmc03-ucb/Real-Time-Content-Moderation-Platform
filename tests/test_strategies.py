import pytest

from moderation.strategies import ab


def test_the_same_stream_always_gets_the_same_bucket():
    assert ab.pick("stream_9").name == ab.pick("stream_9").name


def test_both_buckets_get_used():
    names = {ab.pick(f"stream_{i}").name for i in range(50)}
    assert names == {"A", "B"}


def test_risky_users_face_lower_thresholds():
    strategy = ab.Strategy("A", delete_threshold=0.9, escalate_threshold=0.6,
                           risk_bonus=0.3)
    clean = ab.thresholds_for(strategy, 0.0)
    risky = ab.thresholds_for(strategy, 3.0)
    assert clean["delete"] == 0.9
    assert risky["delete"] == pytest.approx(0.6)


def test_strategies_load_from_the_database_rows():
    rows = [{"name": "B", "config_json": {"delete_threshold": 0.5}},
            {"name": "A", "config_json": {}}]
    loaded = ab.from_rows(rows)
    assert [s.name for s in loaded] == ["A", "B"]
    assert loaded[1].delete_threshold == 0.5
