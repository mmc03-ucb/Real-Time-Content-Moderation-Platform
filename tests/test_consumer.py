import json
from moderation.ingest.consumer import parse


def test_parse_reverses_producer_encoding():
    original = {"user_id": "u1", "text": "hi"}
    raw = json.dumps(original).encode("utf-8")
    assert parse(raw) == original