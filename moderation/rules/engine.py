"""
The cheap half of moderation: plain pattern rules, run before the ML model.

Rules are ordered by priority and the first match wins, so an expensive model
call only happens for messages nothing simple could decide.
"""

import re
from typing import List, Optional, Tuple

from moderation.rules.models import (FREQUENCY, KEYWORD, LINK, NEW_ACCOUNT,
                                     REGEX, Rule, RuleHit)

URL_PATTERN = re.compile(r"https?://\S+|\bwww\.\S+|\b\S+\.(?:com|net|org|io|xyz|example)\b",
                         re.IGNORECASE)


class RuleEngine:
    """Holds one snapshot of the ruleset and answers questions about a message."""

    def __init__(self, rules: List[Rule], version: int = 0):
        self.version = version
        self.rules = sorted((r for r in rules if r.enabled), key=lambda r: r.priority)
        # Regexes are compiled once here instead of on every message.
        self._compiled = {
            r.id: re.compile(r.pattern, re.IGNORECASE)
            for r in self.rules if r.rule_type == REGEX and r.pattern
        }

    def rules_for(self, stream_id: str) -> List[Rule]:
        """Global rules plus any rule written for this one stream."""
        return [r for r in self.rules if r.stream_id in (None, stream_id)]

    def evaluate(self, message: dict) -> List[RuleHit]:
        """Return every rule this message trips, most important first."""
        hits = []
        for rule in self.rules_for(message.get("stream_id", "")):
            if self._matches(rule, message):
                hits.append(RuleHit(rule.id, rule.name, rule.version, rule.action))
        return hits

    def frequency_limit(self, stream_id: str) -> Optional[Tuple[int, str]]:
        """
        How many messages a user may send per 10 seconds, and what to do if
        they go over. Comes from a rule row so it can be tuned without a deploy.
        """
        for rule in self.rules_for(stream_id):
            if rule.rule_type == FREQUENCY and rule.threshold:
                return int(rule.threshold), rule.action
        return None

    def _matches(self, rule: Rule, message: dict) -> bool:
        text = message.get("text", "")

        if rule.rule_type == KEYWORD:
            # Pattern is a comma separated word list, e.g. "idiot,moron".
            words = [w.strip().lower() for w in (rule.pattern or "").split(",") if w.strip()]
            lowered = text.lower()
            return any(w in lowered for w in words)

        if rule.rule_type == REGEX:
            compiled = self._compiled.get(rule.id)
            return bool(compiled and compiled.search(text))

        if rule.rule_type == LINK:
            # threshold is how many links are allowed before the rule fires.
            allowed = int(rule.threshold or 0)
            return len(URL_PATTERN.findall(text)) > allowed

        if rule.rule_type == NEW_ACCOUNT:
            # Accounts younger than `threshold` days may not post links.
            age = message.get("client_meta", {}).get("account_age_days")
            if age is None:
                return False
            return age < (rule.threshold or 0) and bool(URL_PATTERN.search(text))

        # frequency rules are enforced by the rate limiter, not by text matching.
        return False
