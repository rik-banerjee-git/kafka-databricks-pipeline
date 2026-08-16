from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "click-event",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="clickstream-test-consumer",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Waiting for events...")

for message in consumer:
    print("Received event:")
    print(message.value)