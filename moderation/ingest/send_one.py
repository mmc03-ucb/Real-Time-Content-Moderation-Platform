"""Dev helper: put one message on the chat topic so you can see the pipe works."""

from moderation.ingest.generator import make_message
from moderation.ingest.producer import build_producer, flush, publish

if __name__ == "__main__":
    producer = build_producer()
    message = make_message("viewer_1", "hello from my first producer")
    publish(producer, message)
    flush(producer)
    print(f"Sent message {message['msg_id']}")
