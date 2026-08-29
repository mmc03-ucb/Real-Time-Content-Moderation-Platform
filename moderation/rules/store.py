"""
Hot reloading of the ruleset.

Rules live in MySQL, but reading MySQL for every message would be silly. Instead
each worker holds a snapshot and watches a single version number in Redis. When
someone edits a rule the number goes up and every worker picks up the change
within a few seconds, with no restart and no dropped messages.
"""

import time
from typing import Callable, List

from moderation.rules.engine import RuleEngine
from moderation.rules.models import Rule

VERSION_KEY = "rules:version"


def read_version(redis_client) -> int:
    """Current ruleset version, or 0 if nobody has published one yet."""
    raw = redis_client.get(VERSION_KEY)
    return int(raw) if raw else 0


def bump_version(redis_client) -> int:
    """Tell every worker that the rules changed."""
    return int(redis_client.incr(VERSION_KEY))


class RuleStore:
    """A worker's live view of the rules."""

    def __init__(self,
                 load_rules: Callable[[], List[Rule]],
                 read_version: Callable[[], int],
                 poll_seconds: float = 3.0,
                 clock: Callable[[], float] = time.monotonic):
        self._load_rules = load_rules
        self._read_version = read_version
        self._poll_seconds = poll_seconds
        self._clock = clock
        self._checked_at = float("-inf")
        self._version = -1
        self._engine = RuleEngine([])
        self.reloads = 0
        self.refresh()

    def current(self) -> RuleEngine:
        """The ruleset to use right now, refreshed if the poll interval has passed."""
        if self._clock() - self._checked_at >= self._poll_seconds:
            self.refresh()
        return self._engine

    def refresh(self) -> bool:
        """Check the version key and reload only if it moved. Returns True if it did."""
        self._checked_at = self._clock()
        version = self._read_version()
        if version == self._version:
            return False
        self._engine = RuleEngine(self._load_rules(), version=version)
        self._version = version
        self.reloads += 1
        return True
