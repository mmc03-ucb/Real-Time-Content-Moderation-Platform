"""
Raid detection.

A raid is a crowd of brand new accounts hitting one stream at the same time.
We count how many different new accounts a stream sees in a ten second bucket;
past the threshold the stream goes into raid mode and new accounts are held
back until it calms down.
"""

WINDOW_SECONDS = 10
RAID_MODE_SECONDS = 60
NEW_ACCOUNT_DAYS = 7

REPLIES = 3  # how many answers queue_sighting() puts on the wire


def is_new_account(message: dict) -> bool:
    age = message.get("client_meta", {}).get("account_age_days")
    return age is not None and age < NEW_ACCOUNT_DAYS


def mode_key(stream_id: str) -> str:
    return f"raid_mode:{stream_id}"


def queue_sighting(pipe, message: dict, now: float) -> None:
    """Note a new account on a stream and count the distinct ones in this window."""
    bucket = f"raid:{message['stream_id']}:{int(now // WINDOW_SECONDS)}"
    pipe.sadd(bucket, message["user_id"])   # a set, so one loud account counts once
    pipe.expire(bucket, WINDOW_SECONDS * 2)
    pipe.scard(bucket)


def unique_new_users(replies) -> int:
    return replies[2]


def queue_mode_check(pipe, stream_id: str) -> None:
    pipe.exists(mode_key(stream_id))


def queue_start_mode(pipe, stream_id: str) -> None:
    """Put a stream into raid mode. It lifts on its own after a quiet minute."""
    pipe.set(mode_key(stream_id), "1", ex=RAID_MODE_SECONDS)
