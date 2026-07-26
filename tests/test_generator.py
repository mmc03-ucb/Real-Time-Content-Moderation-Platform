from moderation.ingest.generator import make_message

def test_message_has_required_fields():
    msg = make_message("viewer_1", "hello stream")
    assert set(msg) == {"message_id", "user_id", "text", "created_at"}
    assert msg["user_id"] == "viewer_1"
    assert msg["text"] == "hello stream"

def test_message_ids_are_unique():
    ids = {make_message("u", "hi")["message_id"] for _ in range(100)}
    assert len(ids) == 100