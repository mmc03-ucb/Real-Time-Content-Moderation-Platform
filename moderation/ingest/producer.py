import json
# pyrefly: ignore [missing-import] for VScode
from confluent_kafka import Producer

TOPIC = "chat-messages"

def build_producer(servers: str = "localhost:9092") -> Producer:
    # Create a Kafka producer connected to the given broker
    return Producer({"bootstrap.servers": servers})
    
def publish(producer: Producer, message: dict) -> None:
    # Serialize a message to json bytes and send it to topic
    payload = json.dumps(message).encode("utf-8")
    producer.produce(TOPIC, value=payload)
    producer.flush()
