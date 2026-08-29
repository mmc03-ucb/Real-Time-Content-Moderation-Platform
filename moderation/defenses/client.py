"""One Redis connection per worker."""

import redis

from moderation.config import settings


def build_redis(url: str = None):
    """Connect to Redis. decode_responses keeps values as strings, not bytes."""
    return redis.Redis.from_url(url or settings.redis_url, decode_responses=True)
