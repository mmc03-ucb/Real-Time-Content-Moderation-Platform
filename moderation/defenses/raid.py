"""
Raid detection.

A raid is a crowd of brand new accounts hitting one stream at the same time.
We count how many different new accounts a stream sees in a ten second bucket;
past the threshold the stream goes into raid mode and new accounts are held
back until it calms down.
"""

import time

WINDOW_SECONDS = 10
RAID_MODE_SECONDS = 60
NEW_ACCOUNT_DAYS = 7


def is_new_account(message: dict) -> bool:
    age = message.get("client_meta", {}).get("account_age_days")
    return age is not None and age < NEW_ACCOUNT_DAYS


def observe(redis_client, message: dict, threshold: int = 25, now: float = None) -> bool:
    """
    Note a new account showing up on a stream. Returns True if that tipped the
    stream into raid mode.
    """
    if not is_new_account(message):
        return False

    now = time.time() if now is None else now
    bucket = int(now // WINDOW_SECONDS)
    stream_id = message["stream_id"]
    key = f"raid:{stream_id}:{bucket}"

    pipe = redis_client.pipeline()
    pipe.sadd(key, message["user_id"])   # a set, so one loud account counts once
    pipe.expire(key, WINDOW_SECONDS * 2)
    pipe.scard(key)
    unique_new_users = pipe.execute()[2]

    if unique_new_users >= threshold:
        redis_client.set(mode_key(stream_id), "1", ex=RAID_MODE_SECONDS)
        return True
    return False


def mode_key(stream_id: str) -> str:
    return f"raid_mode:{stream_id}"


def in_raid_mode(redis_client, stream_id: str) -> bool:
    return bool(redis_client.exists(mode_key(stream_id)))
