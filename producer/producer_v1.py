from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

topic = "click-event"

for i in range(50):
    event = {
    "user_id": "user-003",
    "event_type": "page_view",
    "page": "/home",
    "timestamp": time.time()
        }

    producer.send(topic, value=event)

producer.flush()

print("Event sent successfully!")

producer.close()