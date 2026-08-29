"""
A/B testing for moderation policy.

Every stream is put in a bucket by hashing its id, so a stream always gets the
same settings and the split stays even. Decisions carry their bucket's name,
which is what lets the dashboard compare how each policy performed.
"""

import hashlib
from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class Strategy:
    """One set of moderation settings under test."""

    name: str
    delete_threshold: float = 0.9     # score at or above this is removed outright
    escalate_threshold: float = 0.6   # score at or above this goes to a human
    risk_bonus: float = 0.1           # how much stricter repeat offenders get


# Used until MySQL is reachable. A is today's policy, B is the challenger.
DEFAULT = [
    Strategy("A", delete_threshold=0.9, escalate_threshold=0.6, risk_bonus=0.10),
    Strategy("B", delete_threshold=0.8, escalate_threshold=0.5, risk_bonus=0.15),
]


def from_rows(rows: List[dict]) -> List[Strategy]:
    """Turn the `strategies` table into Strategy objects."""
    strategies = []
    for row in rows:
        config = row.get("config_json") or {}
        strategies.append(Strategy(
            name=row["name"],
            delete_threshold=float(config.get("delete_threshold", 0.9)),
            escalate_threshold=float(config.get("escalate_threshold", 0.6)),
            risk_bonus=float(config.get("risk_bonus", 0.1)),
        ))
    return sorted(strategies, key=lambda s: s.name) or list(DEFAULT)


def pick(stream_id: str, strategies: List[Strategy] = None) -> Strategy:
    """
    Which bucket this stream belongs to.

    md5 rather than Python's built-in hash, because that one is randomised per
    process and every worker has to agree on the answer.
    """
    strategies = strategies or DEFAULT
    digest = hashlib.md5(stream_id.encode("utf-8")).hexdigest()
    return strategies[int(digest, 16) % len(strategies)]


def thresholds_for(strategy: Strategy, risk_score: float) -> Dict[str, float]:
    """
    The thresholds to use for one message.

    Someone with a recent history of violations is judged more strictly, up to
    the strategy's risk bonus.
    """
    penalty = strategy.risk_bonus * min(risk_score, 3.0) / 3.0
    return {
        "delete": max(0.0, strategy.delete_threshold - penalty),
        "escalate": max(0.0, strategy.escalate_threshold - penalty),
    }
