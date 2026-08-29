"""Dev helper: print every message on the chat topic as it arrives."""

from moderation.config import settings
from moderation.ingest.consumer import build_consumer, parse

if __name__ == "__main__":
    consumer = build_consumer(group="tail-chat")
    consumer.subscribe([settings.chat_topic])
    print("Listening for messages. Ctrl-C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue
            data = parse(msg.value())
            print(f"[{data['stream_id']}] {data['user_id']}: {data['text']}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
