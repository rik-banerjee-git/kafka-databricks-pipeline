from anyio import sleep
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import signal
import sys
import logging


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "clickstream"
CONSUMER_GROUP = "clickstream-processor-v1"


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Consumer
# ---------------------------------------------------------

consumer = KafkaConsumer(
    TOPIC,

    bootstrap_servers=BOOTSTRAP_SERVERS,

    # Consumer group
    group_id=CONSUMER_GROUP,

    # Deserialize Kafka message value
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),

    # Deserialize key
    key_deserializer=lambda key: (
        key.decode("utf-8") if key else None
    ),

    # Production-style offset management
    enable_auto_commit=False,

    # For first run, read existing messages
    # Change to "latest" in a production deployment
    auto_offset_reset="earliest",

    # Don't return immediately with tiny batches
    max_poll_records=100,

    # Consumer should send heartbeats regularly
    session_timeout_ms=30000,

    # Maximum time allowed between poll() calls
    max_poll_interval_ms=300000,
)


# ---------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------

running = True


def shutdown_handler(signum, frame):
    global running

    logger.info("Shutdown signal received...")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ---------------------------------------------------------
# Event processing
# ---------------------------------------------------------

def process_event(event, key, partition, offset):

    logger.info(
        "Processing event | "
        "key=%s | partition=%s | offset=%s | event_id=%s | "
        "user_id=%s | event_type=%s | page=%s",
        key,
        partition,
        offset,
        event.get("event_id"),
        event.get("user_id"),
        event.get("event_type"),
        event.get("page"),
    )

    # -----------------------------------------------------
    # This is where our actual business logic will go.
    #
    # Example later:
    #
    # write_to_database(event)
    # send_to_databricks(event)
    # update_user_profile(event)
    # -----------------------------------------------------

    return True


# ---------------------------------------------------------
# Consumer loop
# ---------------------------------------------------------

logger.info("Starting Kafka consumer...")
logger.info("Topic: %s", TOPIC)
logger.info("Consumer Group: %s", CONSUMER_GROUP)

try:

    while running:

        records = consumer.poll(
            timeout_ms=1000,
            max_records=100
        )

        if not records:
            continue

        for topic_partition, messages in records.items():

            for message in messages:

                try:

                    success = process_event(
                        event=message.value,
                        key=message.key,
                        partition=message.partition,
                        offset=message.offset,
                    )

                    if success:

                        # Commit ONLY after successful processing
                        consumer.commit()
                        sleep(1)
                        logger.info(
                            "Committed | partition=%s | offset=%s",
                            message.partition,
                            message.offset,
                        )

                except Exception as error:

                    logger.exception(
                        "Failed processing event | "
                        "partition=%s | offset=%s | error=%s",
                        message.partition,
                        message.offset,
                        error,
                    )

                    # Important:
                    # Do NOT commit failed message.
                    #
                    # Kafka will allow us to process it again.
                    continue


except KafkaError as error:

    logger.exception(
        "Kafka error: %s",
        error
    )

finally:

    logger.info("Closing Kafka consumer...")

    consumer.close()

    logger.info("Consumer stopped.")