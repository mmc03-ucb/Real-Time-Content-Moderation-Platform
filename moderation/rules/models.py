"""What a moderation rule looks like once it is loaded out of MySQL."""

from dataclasses import dataclass
from typing import Optional

# What a rule can do to a message.
ALLOW = "allow"
DELETE = "delete"
SHADOW = "shadow"      # message is hidden from others but the author still sees it
ESCALATE = "escalate"  # send it to a human

# The kinds of rule the engine knows how to evaluate.
KEYWORD = "keyword"
REGEX = "regex"
LINK = "link"
NEW_ACCOUNT = "new_account"
FREQUENCY = "frequency"


@dataclass(frozen=True)
class Rule:
    id: int
    name: str
    rule_type: str
    pattern: Optional[str]
    threshold: Optional[float]
    action: str
    priority: int          # lower number is checked first
    enabled: bool
    stream_id: Optional[str]  # None means the rule applies to every stream
    version: int


@dataclass(frozen=True)
class RuleHit:
    """A rule that matched, kept around so any decision can be explained."""

    rule_id: int
    rule_name: str
    rule_version: int
    action: str

    def as_dict(self) -> dict:
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "rule_version": self.rule_version,
            "action": self.action,
        }
