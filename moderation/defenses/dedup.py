"""
Copy-paste spam check.

A spammer sending the same line over and over is the easiest thing to catch:
hash the user and the text, and refuse the second copy within the TTL.
"""

import hashlib


def _key(user_id: str, text: str) -> str:
    digest = hashlib.sha1(f"{user_id}|{text.strip().lower()}".encode("utf-8")).hexdigest()
    return f"dedup:{digest}"


def is_duplicate(redis_client, user_id: str, text: str, ttl_seconds: int = 30) -> bool:
    """
    True if this exact message was already seen recently.

    SET NX is a single atomic call, so two workers racing on the same message
    cannot both decide theirs is the original.
    """
    first_time = redis_client.set(_key(user_id, text), "1", nx=True, ex=ttl_seconds)
    return not first_time
