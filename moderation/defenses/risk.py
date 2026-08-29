"""
User reputation.

A score goes up every time someone breaks a rule and fades on its own, so one
bad night does not follow an account around forever. Risky users are held to
stricter thresholds by the pipeline.
"""

HALF_LIFE_SECONDS = 3600.0  # a score halves every hour of good behaviour


def key(user_id: str) -> str:
    return f"risk:{user_id}"


def decayed(score: float, updated_at: float, now: float) -> float:
    """What an old score is worth today."""
    elapsed = max(0.0, now - updated_at)
    return score * (0.5 ** (elapsed / HALF_LIFE_SECONDS))


def queue_read(pipe, user_id: str) -> None:
    pipe.hmget(key(user_id), "score", "updated_at")


def read(reply, now: float) -> float:
    """Turn the stored score into today's score."""
    score, updated_at = reply
    if score is None:
        return 0.0
    return decayed(float(score), float(updated_at or now), now)


def queue_violation(pipe, user_id: str, current: float, amount: float,
                    now: float) -> None:
    """Add to a user's faded score and save it."""
    pipe.hset(key(user_id), mapping={"score": current + amount, "updated_at": now})
    pipe.expire(key(user_id), int(HALF_LIFE_SECONDS * 24))  # forget quiet accounts
