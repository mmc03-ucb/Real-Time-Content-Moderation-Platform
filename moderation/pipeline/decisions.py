"""The verdict a message gets, and the short codes that explain why."""

from dataclasses import dataclass, field
from typing import List, Optional

# What we do with the message.
ALLOW = "allow"
DELETE = "delete"
SHADOW = "shadow"
ESCALATE = "escalate"

# Why we did it. Short codes so they group nicely in dashboards.
CLEAN = "clean"                  # nothing tripped
DUPLICATE = "duplicate"          # same text from the same user, seconds apart
RATE_LIMIT = "rate_limit"        # sending too fast
RAID_MODE = "raid_mode"          # new account during a raid on this stream
RULE_MATCH = "rule_match"        # a rule in MySQL said so
ML_TOXIC = "ml_toxic"            # the model was confident
ML_UNCERTAIN = "ml_uncertain"    # the model was unsure, a human decides
ML_UNAVAILABLE = "ml_unavailable"  # the model was down, rules only


@dataclass
class Decision:
    """One verdict, with everything needed to explain and audit it later."""

    msg_id: str
    stream_id: str
    user_id: str
    action: str
    reason_code: str
    strategy: str = "A"
    rule_id: Optional[int] = None
    ml_score: Optional[float] = None
    latency_ms: float = 0.0
    rule_hits: List[dict] = field(default_factory=list)

    @property
    def is_violation(self) -> bool:
        """Anything that was not simply allowed counts against the user."""
        return self.action in (DELETE, SHADOW)

    def as_event(self) -> dict:
        """The shape published to the moderation.decisions topic."""
        return {
            "msg_id": self.msg_id,
            "stream_id": self.stream_id,
            "user_id": self.user_id,
            "action": self.action,
            "reason_code": self.reason_code,
            "strategy": self.strategy,
            "rule_id": self.rule_id,
            "ml_score": self.ml_score,
            "latency_ms": self.latency_ms,
            "rule_hits": self.rule_hits,
        }
