from moderation.rules.models import Rule
from moderation.rules.store import RuleStore


def make_rules(n):
    return [Rule(id=i, name=f"r{i}", rule_type="keyword", pattern="x", threshold=None,
                 action="delete", priority=10, enabled=True, stream_id=None, version=1)
            for i in range(n)]


class Fixture:
    """Stands in for MySQL and Redis so the reload logic can be tested on its own."""

    def __init__(self):
        self.version = 1
        self.count = 1
        self.loads = 0
        self.now = 0.0

    def load(self):
        self.loads += 1
        return make_rules(self.count)

    def store(self, poll_seconds=3.0):
        return RuleStore(self.load, lambda: self.version, poll_seconds,
                         clock=lambda: self.now)


def test_rules_load_once_on_start_up():
    fix = Fixture()
    store = fix.store()
    assert fix.loads == 1
    assert len(store.current().rules) == 1


def test_rules_are_not_reloaded_while_the_version_is_unchanged():
    fix = Fixture()
    store = fix.store(poll_seconds=0)
    for _ in range(5):
        store.current()
    assert fix.loads == 1


def test_a_new_version_swaps_the_ruleset_in():
    fix = Fixture()
    store = fix.store(poll_seconds=0)
    fix.count, fix.version = 3, 2
    assert len(store.current().rules) == 3
    assert store.reloads == 2


def test_the_version_is_only_checked_once_per_interval():
    fix = Fixture()
    store = fix.store(poll_seconds=3.0)
    fix.version = 2
    store.current()          # too soon, still on the old snapshot
    assert store.reloads == 1
    fix.now = 4.0
    store.current()
    assert store.reloads == 2
