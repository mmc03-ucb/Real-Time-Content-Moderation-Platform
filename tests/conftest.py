import fakeredis
import pytest


@pytest.fixture
def fake_redis():
    """An in-memory stand-in for Redis, so tests need no running server."""
    return fakeredis.FakeRedis(decode_responses=True)
