from moderation.ingest.consumer import build_consumer, parse, TOPIC

if __name__ == "__main__":
    consumer = build_consumer()
    consumer.subscribe([TOPIC])
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
            print(f"Consumed message from {data['user_id']}: {data['text']}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()