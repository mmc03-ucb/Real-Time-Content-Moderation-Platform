"""
Sliding window rate limiting.

Every message a user sends is stamped into a Redis sorted set, scored by time.
Old stamps are dropped before counting, so "10 messages in the last 10 seconds"
is exactly what gets enforced, with no bursts sneaking through on a window edge.
"""

import time
import uuid


def allow(redis_client, scope: str, limit: int,
          window_seconds: float = 10.0, now: float = None) -> bool:
    """True if this message is under the limit, False if the sender is going too fast."""
    now = time.time() if now is None else now
    key = f"rate:{scope}"

    pipe = redis_client.pipeline()
    pipe.zremrangebyscore(key, 0, now - window_seconds)   # forget old messages
    pipe.zadd(key, {f"{now}:{uuid.uuid4().hex[:8]}": now})  # record this one
    pipe.zcard(key)                                        # how many are left
    pipe.expire(key, int(window_seconds) + 1)              # tidy up idle users
    count = pipe.execute()[2]

    return count <= limit
