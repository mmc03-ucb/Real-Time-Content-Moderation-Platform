"""
Asking Redis everything about a batch in one go.

Checking each message on its own meant six or seven network round trips per
message, and that was most of the cost of moderating one. The same questions
asked for a whole batch travel as a single pipelined request, which is where
most of this project's throughput comes from.
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple

from moderation.defenses import dedup, raid, rate_limit, risk


@dataclass
class Signals:
    """What Redis knows about one message."""

    duplicate: bool = False
    over_rate_limit: bool = False
    raid_mode: bool = False
    new_account: bool = False
    risk_score: float = 0.0


@dataclass
class BatchSignals:
    """The answers for a whole batch, plus any raid that started inside it."""

    per_message: Dict[str, Signals] = field(default_factory=dict)
    new_raids: Set[str] = field(default_factory=set)

    def __getitem__(self, msg_id: str) -> Signals:
        return self.per_message[msg_id]


def gather(redis_client, messages: List[dict], limit: int,
           window_seconds: float, raid_threshold: int,
           now: float = None) -> BatchSignals:
    """Answer the dedup, rate limit, raid and reputation questions for a batch."""
    now = time.time() if now is None else now
    if not messages:
        return BatchSignals()

    streams = list(dict.fromkeys(m["stream_id"] for m in messages))
    users = list(dict.fromkeys(m["user_id"] for m in messages))

    # ---- everything we want to know, queued up as one request
    pipe = redis_client.pipeline()
    asked = []
    for message in messages:
        checked_dedup = dedup.queue(pipe, message["user_id"], message["text"])
        rate_limit.queue(pipe, f"user:{message['user_id']}", window_seconds, now)
        is_new = raid.is_new_account(message)
        if is_new:
            raid.queue_sighting(pipe, message, now)
        asked.append((message, checked_dedup, is_new))

    for stream_id in streams:
        raid.queue_mode_check(pipe, stream_id)
    for user_id in users:
        risk.queue_read(pipe, user_id)

    replies = pipe.execute()

    # ---- unpack the answers in the order they were asked for
    cursor = 0

    def take(count: int):
        nonlocal cursor
        chunk = replies[cursor:cursor + count]
        cursor += count
        return chunk

    batch = BatchSignals()
    for message, checked_dedup, is_new in asked:
        entry = Signals(new_account=is_new)
        if checked_dedup:
            entry.duplicate = dedup.was_duplicate(take(1)[0])
        entry.over_rate_limit = rate_limit.count(take(rate_limit.REPLIES)) > limit
        if is_new and raid.unique_new_users(take(raid.REPLIES)) >= raid_threshold:
            batch.new_raids.add(message["stream_id"])
        batch.per_message[message["msg_id"]] = entry

    raiding = {s for s in streams if take(1)[0]} | batch.new_raids
    risk_by_user = {user_id: risk.read(take(1)[0], now) for user_id in users}

    for message, _, _ in asked:
        entry = batch.per_message[message["msg_id"]]
        entry.raid_mode = message["stream_id"] in raiding
        entry.risk_score = risk_by_user[message["user_id"]]

    return batch


def commit(redis_client, new_raids: Set[str],
           violations: List[Tuple[str, float, float]], now: float = None) -> None:
    """
    Write back what the batch changed: raid mode for any stream that just tipped
    over, and reputation for everyone who broke a rule. One more round trip.
    """
    if not new_raids and not violations:
        return
    now = time.time() if now is None else now

    pipe = redis_client.pipeline()
    for stream_id in new_raids:
        raid.queue_start_mode(pipe, stream_id)
    for user_id, current, amount in violations:
        risk.queue_violation(pipe, user_id, current, amount, now)
    pipe.execute()
