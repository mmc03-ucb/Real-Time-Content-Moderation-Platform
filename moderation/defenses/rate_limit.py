"""
Sliding window rate limiting.

Every message a user sends is stamped into a Redis sorted set, scored by time.
Old stamps are dropped before counting, so "10 messages in the last 10 seconds"
is exactly what gets enforced, with no bursts sneaking through on a window edge.
"""

import uuid

REPLIES = 4  # how many answers queue() puts on the wire


def queue(pipe, scope: str, window_seconds: float, now: float) -> None:
    """Forget old messages, record this one, count what is left in the window."""
    key = f"rate:{scope}"
    pipe.zremrangebyscore(key, 0, now - window_seconds)
    pipe.zadd(key, {f"{now}:{uuid.uuid4().hex[:8]}": now})
    pipe.zcard(key)
    pipe.expire(key, int(window_seconds) + 1)


def count(replies) -> int:
    """How many messages this user has sent inside the window, including this one."""
    return replies[2]
