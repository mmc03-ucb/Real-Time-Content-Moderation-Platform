from moderation.ingest.generator import make_message


def test_message_has_required_fields():
    msg = make_message("viewer_1", "hello stream", stream_id="s1")
    assert set(msg) == {"msg_id", "stream_id", "user_id", "text", "ts", "client_meta"}
    assert msg["user_id"] == "viewer_1"
    assert msg["text"] == "hello stream"
    assert msg["stream_id"] == "s1"


def test_message_ids_are_unique():
    ids = {make_message("u", "hi")["msg_id"] for _ in range(100)}
    assert len(ids) == 100


def test_client_meta_defaults_to_empty():
    assert make_message("u", "hi")["client_meta"] == {}
