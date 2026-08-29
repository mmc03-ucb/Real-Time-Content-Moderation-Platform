"""
The funnel every message goes through.

Order matters. The cheap checks run first and settle most of the traffic for
almost nothing; only what is left costs a call to the toxicity model. Both
halves work on a whole batch at a time: one Redis round trip for the checks,
one model call for the scoring.

    duplicate? -> rate limited? -> raid? -> rules -> model -> verdict
"""

import time
from typing import List, Optional, Tuple

from moderation.defenses import signals as signal_batch
from moderation.obs import metrics
from moderation.obs.tracing import tracer
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
        """Decide a whole batch. Two Redis trips and at most one model call."""
        if not messages:
            return []

        engine = self.rule_store.current()
        metrics.RULES_VERSION.set(engine.version)
        metrics.BATCH_SIZE.observe(len(messages))

        with tracer().start_as_current_span("moderate_batch") as span:
            span.set_attribute("batch.size", len(messages))
            batch = self._ask_redis(engine, messages)
            decided, needs_model = self._run_cheap_checks(engine, messages, batch)
            span.set_attribute("batch.sent_to_model", len(needs_model))
            decided.extend(self._run_model(needs_model, batch))
            self._finish(decided, messages, batch)

        for decision in decided:
            metrics.DECISIONS.labels(decision.action, decision.reason_code,
                                     decision.strategy).inc()
        metrics.MESSAGES.inc(len(messages))
        metrics.MODEL_HEALTHY.set(1 if getattr(self.classifier, "healthy", True) else 0)
        return decided

    # ------------------------------------------------------------------

    def _ask_redis(self, engine, messages):
        """One request that answers dedup, speed, raids and reputation."""
        limit, _ = self._rate_settings(engine, messages[0]["stream_id"])
        with tracer().start_as_current_span("redis_signals"):
            with metrics.STAGE_SECONDS.labels("redis").time():
                return signal_batch.gather(self.redis, messages, limit,
                                           RATE_WINDOW_SECONDS, self.raid_threshold)

    def _run_cheap_checks(self, engine, messages, batch):
        """Settle everything a rule or a Redis counter can answer on its own."""
        decided: List[Decision] = []
        needs_model: List[Tuple[dict, ab.Strategy]] = []
        with tracer().start_as_current_span("cheap_checks"):
            with metrics.STAGE_SECONDS.labels("rules").time():
                for message in messages:
                    strategy = ab.pick(message["stream_id"], self.strategies)
                    early = self._cheap_checks(engine, message, strategy,
                                               batch[message["msg_id"]])
                    if early is not None:
                        decided.append(early)
                    else:
                        needs_model.append((message, strategy))
        return decided, needs_model

    def _run_model(self, needs_model, batch) -> List[Decision]:
        """One model call for whatever the cheap checks could not settle."""
        with tracer().start_as_current_span("classify"):
            with metrics.STAGE_SECONDS.labels("model").time():
                scores = self.classifier.score_batch([m["text"] for m, _ in needs_model])
        return [self._from_score(message, strategy, score, batch[message["msg_id"]])
                for (message, strategy), score in zip(needs_model, scores)]

    def _finish(self, decided, messages, batch) -> None:
        """Stamp each verdict with its latency, then write back what changed."""
        now = time.time()
        by_id = {m["msg_id"]: m for m in messages}
        violations = []
        for decision in decided:
            message = by_id.get(decision.msg_id)
            if message is None:
                continue
            # End to end: from the moment the viewer sent it to the verdict.
            decision.latency_ms = max(0.0, (now - message["ts"]) * 1000)
            metrics.END_TO_END_MS.observe(decision.latency_ms)
            if decision.is_violation:
                violations.append((decision.user_id,
                                   batch[decision.msg_id].risk_score, 1.0))
        signal_batch.commit(self.redis, batch.new_raids, violations, now=now)

    def _cheap_checks(self, engine, message: dict, strategy: ab.Strategy,
                      signals) -> Optional[Decision]:
        """Everything that can be answered without the model. None means carry on."""
        if signals.duplicate:
            return self._decision(message, strategy, DELETE, DUPLICATE)

        if signals.over_rate_limit:
            _, over_limit_action = self._rate_settings(engine, message["stream_id"])
            return self._decision(message, strategy, over_limit_action, RATE_LIMIT)

        # While a raid is on, new accounts are hidden rather than escalated:
        # sending thousands of raid messages to the review queue would bury
        # the moderators.
        if signals.new_account and signals.raid_mode:
            return self._decision(message, strategy, SHADOW, RAID_MODE)

        hits = engine.evaluate(message)
        if hits:
            top = hits[0]
            return self._decision(message, strategy, top.action, RULE_MATCH,
                                  rule_id=top.rule_id,
                                  rule_hits=[h.as_dict() for h in hits])
        return None

    def _from_score(self, message: dict, strategy: ab.Strategy,
                    score: Optional[float], signals) -> Decision:
        """Apply the three way threshold to a model score."""
        if score is None:
            # The model is down. Nothing tripped a rule, so let it through and
            # let the alert on ml_unavailable get someone's attention.
            return self._decision(message, strategy, ALLOW, ML_UNAVAILABLE)

        cuts = ab.thresholds_for(strategy, signals.risk_score)
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
