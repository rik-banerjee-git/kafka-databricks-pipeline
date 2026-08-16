import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "clickstream"


# ============================================================
# Faker
# ============================================================

fake = Faker("en_IN")


# ============================================================
# Static/reference data
# ============================================================

EVENT_TYPES = [
    "page_view",
    "product_view",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "purchase",
    "login",
    "logout",
]


EVENT_WEIGHTS = [
    40,  # page_view
    20,  # product_view
    10,  # search
    8,   # add_to_cart
    4,   # remove_from_cart
    5,   # checkout_started
    3,   # purchase
    5,   # login
    5,   # logout
]


DEVICE_TYPES = [
    "mobile",
    "desktop",
    "tablet",
]


OS_BY_DEVICE = {
    "mobile": ["Android", "iOS"],
    "desktop": ["Windows", "macOS", "Linux"],
    "tablet": ["Android", "iPadOS"],
}


BROWSERS = [
    "Chrome",
    "Firefox",
    "Safari",
    "Edge",
]


PRODUCT_CATEGORIES = [
    "electronics",
    "fashion",
    "home",
    "books",
    "sports",
    "beauty",
]


PAGES = [
    "/",
    "/home",
    "/products",
    "/products/laptop",
    "/products/mobile",
    "/products/headphones",
    "/cart",
    "/checkout",
    "/search",
]


# ============================================================
# Producer
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    # Kafka key
    key_serializer=lambda key: key.encode("utf-8"),

    # JSON message
    value_serializer=lambda value: json.dumps(
        value,
        separators=(",", ":")
    ).encode("utf-8"),

    # Reliability
    acks="all",

    # Retry failed sends
    retries=5,

    # Improve batching
    linger_ms=10,

    # Compress network payload
    compression_type="gzip",

    # Prevent duplicate records caused by retries
    enable_idempotence=True,
)


# ============================================================
# Generate user
# ============================================================

def generate_user():
    return {
        "user_id": f"user-{random.randint(1, 1_000_000):06d}"
    }


# ============================================================
# Generate session
# ============================================================

def generate_session():
    return {
        "session_id": str(uuid.uuid4())
    }


# ============================================================
# Generate device
# ============================================================

def generate_device():

    device_type = random.choice(DEVICE_TYPES)

    return {
        "device_type": device_type,
        "os": random.choice(OS_BY_DEVICE[device_type]),
        "browser": random.choice(BROWSERS),
    }


# ============================================================
# Generate page
# ============================================================

def generate_page():

    page = random.choice(PAGES)

    return {
        "url": page,
        "referrer": random.choice(PAGES),
    }


# ============================================================
# Generate product
# ============================================================

def generate_product():

    return {
        "product_id": f"PROD-{random.randint(1, 100_000):06d}",
        "category": random.choice(PRODUCT_CATEGORIES),
        "price": round(random.uniform(100, 150_000), 2),
        "currency": "INR",
    }


# ============================================================
# Generate complete event
# ============================================================

def generate_event():

    user = generate_user()
    session = generate_session()
    device = generate_device()
    page = generate_page()
    product = generate_product()

    event_name = random.choices(
        EVENT_TYPES,
        weights=EVENT_WEIGHTS,
        k=1
    )[0]

    now = datetime.now(timezone.utc)

    event = {

        # ----------------------------------------------------
        # Event metadata
        # ----------------------------------------------------

        "event_id": str(uuid.uuid4()),

        "event_name": event_name,

        "event_version": "1.0",

        "event_time": now.isoformat(),

        # ----------------------------------------------------
        # User
        # ----------------------------------------------------

        "user": user,

        # ----------------------------------------------------
        # Session
        # ----------------------------------------------------

        "session": session,

        # ----------------------------------------------------
        # Device
        # ----------------------------------------------------

        "device": device,

        # ----------------------------------------------------
        # Geo
        # ----------------------------------------------------

        "geo": {
            "country": "IN",
            "city": fake.city(),
        },

        # ----------------------------------------------------
        # Page
        # ----------------------------------------------------

        "page": page,

        # ----------------------------------------------------
        # Product
        # ----------------------------------------------------

        "product": product,

        # ----------------------------------------------------
        # Source
        # ----------------------------------------------------

        "source": {
            "application": "web",
            "environment": "dev",
            "producer": "clickstream-producer",
        },
    }

    return event


# ============================================================
# Delivery callback
# ============================================================

def on_send_success(record_metadata):

    print(
        f"SUCCESS | "
        f"topic={record_metadata.topic} | "
        f"partition={record_metadata.partition} | "
        f"offset={record_metadata.offset}"
    )


def on_send_error(excp):

    print(f"ERROR | Kafka delivery failed: {excp}")


# ============================================================
# Main producer loop
# ============================================================

def main():

    print("Starting clickstream producer...")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print("-" * 80)

    try:

        while True:

            event = generate_event()

            # IMPORTANT:
            # user_id is the Kafka partition key.
            #
            # All events for the same user will normally
            # be routed to the same partition.

            user_id = event["user"]["user_id"]

            future = producer.send(
                KAFKA_TOPIC,
                key=user_id,
                value=event,
            )

            future.add_callback(on_send_success)
            future.add_errback(on_send_error)

            print(
                f"EVENT | "
                f"user={user_id} | "
                f"type={event['event_name']} | "
                f"session={event['session']['session_id']}"
            )

            # Simulate real user traffic
            time.sleep(random.uniform(0.05, 0.5))

    except KeyboardInterrupt:

        print("\nStopping producer...")

    finally:

        producer.flush()
        producer.close()

        print("Producer closed.")


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()