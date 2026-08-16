import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError


# ============================================================
# CONFIGURATION
# ============================================================

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "clickstream"

PRODUCER_ID = "clickstream-producer"

EVENTS_PER_SECOND = 5


# ============================================================
# INITIALIZATION
# ============================================================

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,

    # JSON serialization
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),

    # Reliability
    acks="all",
    retries=5,

    # Prevent duplicate sends caused by producer retries
    enable_idempotence=True,

    # Compression
    compression_type="gzip",

    # Batch records before sending
    linger_ms=10,
    batch_size=32 * 1024,

    # Request timeout
    request_timeout_ms=30000,
)


# ============================================================
# TEST DATA
# ============================================================

PAGES = [
    "/",
    "/home",
    "/products",
    "/products/laptop",
    "/products/mobile",
    "/products/headphones",
    "/search",
    "/cart",
    "/checkout",
]

EVENT_TYPES = [
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "search",
]

DEVICE_TYPES = [
    "desktop",
    "mobile",
    "tablet",
]

OPERATING_SYSTEMS = [
    "Windows",
    "macOS",
    "Android",
    "iOS",
    "Linux",
]

BROWSERS = [
    "Chrome",
    "Firefox",
    "Safari",
    "Edge",
]


# ============================================================
# EVENT GENERATOR
# ============================================================

def generate_event():
    """
    Generate one production-style clickstream event.
    """

    user_id = f"user-{random.randint(1, 100000)}"

    session_id = str(uuid.uuid4())

    current_page = random.choice(PAGES)

    event_type = random.choice(EVENT_TYPES)

    event = {
        "event_id": str(uuid.uuid4()),

        "event_type": event_type,

        "event_version": 1,

        "timestamp": datetime.now(timezone.utc).isoformat(),

        "user_id": user_id,

        "session_id": session_id,

        "page": {
            "url": current_page,
            "referrer": random.choice(PAGES),
        },

        "device": {
            "type": random.choice(DEVICE_TYPES),
            "os": random.choice(OPERATING_SYSTEMS),
            "browser": random.choice(BROWSERS),
        },

        "source": {
            "application": "web",
            "environment": "development",
            "producer": PRODUCER_ID,
        },
    }

    return event


# ============================================================
# CALLBACK
# ============================================================

def delivery_report(metadata):
    """
    Print Kafka delivery metadata.
    """

    print(
        json.dumps(
            {
                "status": "DELIVERED",
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset,
            }
        )
    )


# ============================================================
# SEND EVENT
# ============================================================

def send_event(event):
    """
    Send event to Kafka.

    IMPORTANT:
    user_id is used as Kafka key.

    Therefore events belonging to the same user
    will normally go to the same partition.
    """

    key = event["user_id"].encode("utf-8")

    try:

        future = producer.send(
            TOPIC,
            key=key,
            value=event,
        )

        metadata = future.get(timeout=10)

        print(
            json.dumps(
                {
                    "status": "SENT",
                    "event_id": event["event_id"],
                    "user_id": event["user_id"],
                    "event_type": event["event_type"],
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                }
            )
        )

    except KafkaError as exc:

        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "event_id": event["event_id"],
                    "error": str(exc),
                }
            )
        )


# ============================================================
# MAIN LOOP
# ============================================================

def main():

    print("Starting clickstream producer...")
    print(f"Topic: {TOPIC}")
    print(f"Bootstrap server: {BOOTSTRAP_SERVERS}")
    print(f"Events/sec: {EVENTS_PER_SECOND}")

    delay = 1 / EVENTS_PER_SECOND

    try:

        while True:

            event = generate_event()

            send_event(event)

            time.sleep(delay)

    except KeyboardInterrupt:

        print("\nProducer shutdown requested.")

    finally:

        print("Flushing producer...")

        producer.flush()

        producer.close()

        print("Producer stopped.")


if __name__ == "__main__":
    main()