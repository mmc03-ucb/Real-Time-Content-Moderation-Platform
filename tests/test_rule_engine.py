from moderation.ingest.generator import make_message
from moderation.rules.engine import RuleEngine
from moderation.rules.models import Rule


def rule(**kwargs):
    base = dict(id=1, name="r", rule_type="keyword", pattern=None, threshold=None,
                action="delete", priority=100, enabled=True, stream_id=None, version=1)
    base.update(kwargs)
    return Rule(**base)


def test_keyword_rule_matches_anywhere_in_the_text():
    engine = RuleEngine([rule(rule_type="keyword", pattern="idiot,moron")])
    assert engine.evaluate(make_message("u", "you are an IDIOT"))
    assert not engine.evaluate(make_message("u", "good game"))


def test_regex_rule_matches():
    engine = RuleEngine([rule(rule_type="regex", pattern=r"free skins")])
    assert engine.evaluate(make_message("u", "FREE SKINS here"))


def test_link_rule_fires_only_past_the_allowance():
    engine = RuleEngine([rule(rule_type="link", threshold=1)])
    assert not engine.evaluate(make_message("u", "clip https://a.example/x"))
    assert engine.evaluate(make_message("u", "https://a.example/x https://b.example/y"))


def test_new_accounts_cannot_post_links():
    engine = RuleEngine([rule(rule_type="new_account", threshold=7, action="shadow")])
    fresh = make_message("u", "join https://spam.example", client_meta={"account_age_days": 1})
    old = make_message("u", "join https://spam.example", client_meta={"account_age_days": 400})
    assert engine.evaluate(fresh)
    assert not engine.evaluate(old)
    # A new account chatting normally is left alone.
    assert not engine.evaluate(make_message("u", "hello", client_meta={"account_age_days": 1}))


def test_disabled_rules_are_ignored():
    engine = RuleEngine([rule(rule_type="keyword", pattern="idiot", enabled=False)])
    assert not engine.evaluate(make_message("u", "idiot"))


def test_hits_come_back_in_priority_order():
    engine = RuleEngine([
        rule(id=2, name="low", rule_type="keyword", pattern="hi", priority=90),
        rule(id=1, name="high", rule_type="keyword", pattern="hi", priority=10),
    ])
    names = [h.rule_name for h in engine.evaluate(make_message("u", "hi"))]
    assert names == ["high", "low"]


def test_stream_rules_only_apply_to_their_stream():
    engine = RuleEngine([rule(rule_type="link", threshold=0, stream_id="stream_0")])
    assert engine.evaluate(make_message("u", "https://a.example", stream_id="stream_0"))
    assert not engine.evaluate(make_message("u", "https://a.example", stream_id="stream_1"))


def test_frequency_limit_is_read_off_a_rule():
    engine = RuleEngine([rule(rule_type="frequency", threshold=10, action="shadow")])
    assert engine.frequency_limit("stream_0") == (10, "shadow")
    assert RuleEngine([]).frequency_limit("stream_0") is None
