from moderation.ingest.generator import make_message
from moderation.ingest.producer import build_producer, publish

if __name__ == "__main__":
    producer = build_producer()
    message = make_message("viewer_1", "hello from my first producer")
    publish(producer, message)
    print(f"Sent message {message['message_id']}")