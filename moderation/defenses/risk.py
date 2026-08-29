"""
User reputation.

A score goes up every time someone breaks a rule and fades on its own, so one
bad night does not follow an account around forever. Risky users are held to
stricter thresholds by the pipeline.
"""

import time

HALF_LIFE_SECONDS = 3600.0  # a score halves every hour of good behaviour


def _decayed(score: float, updated_at: float, now: float) -> float:
    elapsed = max(0.0, now - updated_at)
    return score * (0.5 ** (elapsed / HALF_LIFE_SECONDS))


def get_score(redis_client, user_id: str, now: float = None) -> float:
    """Current risk for a user, after fading whatever they earned earlier."""
    now = time.time() if now is None else now
    score, updated_at = redis_client.hmget(f"risk:{user_id}", "score", "updated_at")
    if score is None:
        return 0.0
    return _decayed(float(score), float(updated_at or now), now)


def add_violation(redis_client, user_id: str, amount: float = 1.0,
                  now: float = None) -> float:
    """Fade the old score, add to it, and save. Returns the new score."""
    now = time.time() if now is None else now
    key = f"risk:{user_id}"
    new_score = get_score(redis_client, user_id, now) + amount
    pipe = redis_client.pipeline()
    pipe.hset(key, mapping={"score": new_score, "updated_at": now})
    pipe.expire(key, int(HALF_LIFE_SECONDS * 24))  # forget accounts that go quiet
    pipe.execute()
    return new_score
