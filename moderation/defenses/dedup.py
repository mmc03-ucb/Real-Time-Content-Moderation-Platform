"""
Copy-paste spam check.

A spammer sending the same line over and over is the easiest thing to catch:
hash the user and the text, and refuse the second copy within the TTL.
"""

import hashlib

# Short reactions like "gg" or "lol" repeat naturally and are not spam,
# so only longer messages are checked for duplicates.
MIN_LENGTH = 15
TTL_SECONDS = 30


def key(user_id: str, text: str) -> str:
    digest = hashlib.sha1(f"{user_id}|{text.strip().lower()}".encode("utf-8")).hexdigest()
    return f"dedup:{digest}"


def queue(pipe, user_id: str, text: str) -> bool:
    """
    Ask Redis to claim this message. Returns False if the message was too short
    to be worth checking.

    SET NX is a single atomic call, so two workers racing on the same message
    cannot both decide theirs is the original.
    """
    if len(text.strip()) < MIN_LENGTH:
        return False
    pipe.set(key(user_id, text), "1", nx=True, ex=TTL_SECONDS)
    return True


def was_duplicate(reply) -> bool:
    """Redis only sets the key for the first copy, so a falsy reply means a repeat."""
    return not reply
