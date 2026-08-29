"""
The funnel every message goes through.

Order matters. The cheap checks run first and settle most of the traffic for
almost nothing; only what is left costs a call to the toxicity model. The model
is then asked about the whole leftover batch in one go.

    duplicate? -> rate limited? -> raid? -> rules -> model -> verdict
"""

import time
from typing import List, Optional, Tuple

from moderation.defenses import dedup, raid, rate_limit, risk
from moderation.pipeline.decisions import (ALLOW, CLEAN, DELETE, DUPLICATE,
                                           ESCALATE, ML_TOXIC, ML_UNAVAILABLE,
                                           ML_UNCERTAIN, RAID_MODE, RATE_LIMIT,
                                           RULE_MATCH, SHADOW, Decision)
from moderation.strategies import ab

DEFAULT_RATE_LIMIT = 10       # messages per user per window
RATE_WINDOW_SECONDS = 10.0
RAID_THRESHOLD = 25           # new accounts on one stream per 10 seconds


class ModerationPipeline:
    """Turns a batch of chat messages into a batch of verdicts."""

    def __init__(self, rule_store, classifier, redis_client,
                 strategies: List[ab.Strategy] = None,
                 raid_threshold: int = RAID_THRESHOLD):
        self.rule_store = rule_store
        self.classifier = classifier
        self.redis = redis_client
        self.strategies = strategies or list(ab.DEFAULT)
        self.raid_threshold = raid_threshold

    def evaluate_batch(self, messages: List[dict]) -> List[Decision]:
        """Decide a whole batch. The model is called at most once per batch."""
        engine = self.rule_store.current()
        decided: List[Decision] = []
        needs_model: List[Tuple[dict, ab.Strategy]] = []

        for message in messages:
            strategy = ab.pick(message["stream_id"], self.strategies)
            early = self._cheap_checks(engine, message, strategy)
            if early is not None:
                decided.append(early)
            else:
                needs_model.append((message, strategy))

        scores = self.classifier.score_batch([m["text"] for m, _ in needs_model])
        for (message, strategy), score in zip(needs_model, scores):
            decided.append(self._from_score(message, strategy, score))

        now = time.time()
        for decision, message in _pair_by_id(decided, messages):
            # End to end: from the moment the viewer sent it to the verdict.
            decision.latency_ms = max(0.0, (now - message["ts"]) * 1000)
            if decision.is_violation:
                risk.add_violation(self.redis, decision.user_id, 1.0, now=now)

        return decided

    # ------------------------------------------------------------------

    def _cheap_checks(self, engine, message: dict,
                      strategy: ab.Strategy) -> Optional[Decision]:
        """Everything that can be answered without the model. None means carry on."""
        user_id, stream_id = message["user_id"], message["stream_id"]

        if dedup.is_duplicate(self.redis, user_id, message["text"]):
            return self._decision(message, strategy, DELETE, DUPLICATE)

        limit, over_limit_action = self._rate_settings(engine, stream_id)
        if not rate_limit.allow(self.redis, f"user:{user_id}", limit,
                                RATE_WINDOW_SECONDS):
            return self._decision(message, strategy, over_limit_action, RATE_LIMIT)

        # Watch for a flood of new accounts. While a raid is on, new accounts
        # are hidden rather than escalated: sending thousands of raid messages
        # to the review queue would bury the moderators.
        raid.observe(self.redis, message, threshold=self.raid_threshold)
        if raid.is_new_account(message) and raid.in_raid_mode(self.redis, stream_id):
            return self._decision(message, strategy, SHADOW, RAID_MODE)

        hits = engine.evaluate(message)
        if hits:
            top = hits[0]
            return self._decision(message, strategy, top.action, RULE_MATCH,
                                  rule_id=top.rule_id,
                                  rule_hits=[h.as_dict() for h in hits])
        return None

    def _from_score(self, message: dict, strategy: ab.Strategy,
                    score: Optional[float]) -> Decision:
        """Apply the three way threshold to a model score."""
        if score is None:
            # The model is down. Nothing tripped a rule, so let it through and
            # let the alert on ml_unavailable get someone's attention.
            return self._decision(message, strategy, ALLOW, ML_UNAVAILABLE)

        user_risk = risk.get_score(self.redis, message["user_id"])
        cuts = ab.thresholds_for(strategy, user_risk)

        if score >= cuts["delete"]:
            return self._decision(message, strategy, DELETE, ML_TOXIC, ml_score=score)
        if score >= cuts["escalate"]:
            return self._decision(message, strategy, ESCALATE, ML_UNCERTAIN,
                                  ml_score=score)
        return self._decision(message, strategy, ALLOW, CLEAN, ml_score=score)

    def _rate_settings(self, engine, stream_id: str):
        """Speed limit for this stream, taken from the rules if one is set."""
        configured = engine.frequency_limit(stream_id)
        if configured:
            return configured
        return DEFAULT_RATE_LIMIT, SHADOW

    @staticmethod
    def _decision(message, strategy, action, reason_code, **extra) -> Decision:
        return Decision(
            msg_id=message["msg_id"],
            stream_id=message["stream_id"],
            user_id=message["user_id"],
            action=action,
            reason_code=reason_code,
            strategy=strategy.name,
            **extra,
        )


def _pair_by_id(decisions: List[Decision], messages: List[dict]):
    """Match each verdict back to the message it came from."""
    by_id = {m["msg_id"]: m for m in messages}
    for decision in decisions:
        message = by_id.get(decision.msg_id)
        if message is not None:
            yield decision, message
