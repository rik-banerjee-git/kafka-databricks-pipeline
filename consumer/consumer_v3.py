import json
import logging
import signal
import sys
import time

from datetime import datetime
from typing import Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition, OffsetAndMetadata
from pydantic import BaseModel, ConfigDict, ValidationError


# ============================================================
# CONFIGURATION
# ============================================================

BOOTSTRAP_SERVERS = "localhost:9092"

TOPIC = "clickstream"

DLQ_TOPIC = "clickstream.DLQ"

CONSUMER_GROUP = "clickstream-processing-group"

CONSUMER_INSTANCE = "consumer-1"

BATCH_SIZE = 100

POLL_TIMEOUT_MS = 1000

MAX_RETRIES = 3

RETRY_DELAY_SECONDS = 1


# ============================================================
# STRUCTURED JSON LOGGER
# ============================================================

class JsonFormatter(logging.Formatter):

    def format(self, record):

        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if hasattr(record, "event_data"):
            log_entry["event"] = record.event_data

        return json.dumps(log_entry)


logger = logging.getLogger("clickstream-consumer")

logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)

handler.setFormatter(JsonFormatter())

logger.addHandler(handler)


# ============================================================
# PYDANTIC EVENT SCHEMA
# ============================================================

class PageInfo(BaseModel):

    url: str

    referrer: Optional[str] = None


class DeviceInfo(BaseModel):

    type: str

    os: str

    browser: str


class SourceInfo(BaseModel):

    application: str

    environment: str

    producer: str


class ClickEvent(BaseModel):

    model_config = ConfigDict(
        extra="forbid"
    )

    event_id: str

    event_type: str

    event_version: int = 1

    timestamp: datetime

    user_id: str

    session_id: str

    page: PageInfo

    device: DeviceInfo

    source: SourceInfo


# ============================================================
# CONSUMER
# ============================================================

consumer = KafkaConsumer(

    TOPIC,

    bootstrap_servers=BOOTSTRAP_SERVERS,

    group_id=CONSUMER_GROUP,

    # We want manual offset management
    enable_auto_commit=False,

    # Start from earliest available data
    auto_offset_reset="earliest",

    # JSON deserialization
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),

    # Batch size
    max_poll_records=BATCH_SIZE,

    # Give processing enough time
    max_poll_interval_ms=300000,

    # Session timeout
    session_timeout_ms=45000,

    # Heartbeat
    heartbeat_interval_ms=15000,
)


# ============================================================
# DLQ PRODUCER
# ============================================================

dlq_producer = KafkaProducer(

    bootstrap_servers=BOOTSTRAP_SERVERS,

    value_serializer=lambda value: json.dumps(
        value
    ).encode("utf-8"),

    key_serializer=lambda value: value.encode("utf-8")
    if value
    else None,

    acks="all",

    retries=5,

    enable_idempotence=True,

    compression_type="gzip",
)


# ============================================================
# SHUTDOWN FLAG
# ============================================================

shutdown_requested = False


def shutdown_handler(signum, frame):

    global shutdown_requested

    logger.info(
        "Shutdown signal received"
    )

    shutdown_requested = True


signal.signal(
    signal.SIGINT,
    shutdown_handler
)

signal.signal(
    signal.SIGTERM,
    shutdown_handler
)


# ============================================================
# VALIDATION
# ============================================================

def validate_event(raw_event):

    """
    Validate raw Kafka event against
    the production event schema.
    """

    return ClickEvent.model_validate(raw_event)


# ============================================================
# BUSINESS PROCESSING
# ============================================================

def process_event(event: ClickEvent):

    """
    Actual business processing.

    Replace this later with:
        - database write
        - API call
        - Databricks
        - Delta Lake
        - analytics processing
        - enrichment
    """

    logger.info(
        "Processing event",
        extra={
            "event_data": {
                "event_id": event.event_id,
                "user_id": event.user_id,
                "event_type": event.event_type,
            }
        },
    )

    # Simulated processing
    time.sleep(0.01)


# ============================================================
# PROCESS BATCH
# ============================================================

def process_batch(messages):

    """
    Process one Kafka batch.

    Returns:
        True  -> batch successfully processed
        False -> processing failed
    """

    logger.info(
        f"Processing batch | messages={len(messages)}"
    )

    for message in messages:

        raw_event = message.value

        event_metadata = {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
        }

        # ----------------------------------------------------
        # VALIDATION
        # ----------------------------------------------------

        try:

            event = validate_event(raw_event)

        except ValidationError as exc:

            logger.error(
                "Invalid event",
                extra={
                    "event_data": {
                        **event_metadata,
                        "error": str(exc),
                    }
                },
            )

            # Poison message -> DLQ
            send_to_dlq(
                message,
                raw_event,
                str(exc),
            )

            continue

        # ----------------------------------------------------
        # RETRYABLE BUSINESS PROCESSING
        # ----------------------------------------------------

        success = False

        for retry_count in range(MAX_RETRIES + 1):

            try:

                process_event(event)

                success = True

                break

            except Exception as exc:

                logger.error(
                    "Event processing failed",
                    extra={
                        "event_data": {
                            **event_metadata,
                            "event_id": event.event_id,
                            "retry_count": retry_count,
                            "error": str(exc),
                        }
                    },
                )

                if retry_count < MAX_RETRIES:

                    time.sleep(
                        RETRY_DELAY_SECONDS
                    )

        # ----------------------------------------------------
        # FAILED AFTER RETRIES
        # ----------------------------------------------------

        if not success:

            send_to_dlq(
                message,
                raw_event,
                "Processing failed after retries",
            )


    return True


# ============================================================
# SEND TO DLQ
# ============================================================

def send_to_dlq(message, event, reason):

    """
    Send failed event to DLQ.
    """

    dlq_event = {

        "original_event": event,

        "error": {

            "reason": reason,

            "original_topic": message.topic,

            "original_partition": message.partition,

            "original_offset": message.offset,

            "failed_at": datetime.utcnow().isoformat() + "Z",

        },
    }

    try:

        future = dlq_producer.send(

            DLQ_TOPIC,

            key=(
                str(event.get("user_id"))
                if event.get("user_id")
                else None
            ),

            value=dlq_event,
        )

        metadata = future.get(
            timeout=10
        )

        logger.error(
            "Event sent to DLQ",
            extra={
                "event_data": {
                    "event_id": event.get(
                        "event_id"
                    ),

                    "topic": message.topic,

                    "partition": message.partition,

                    "offset": message.offset,

                    "dlq_partition": metadata.partition,

                    "dlq_offset": metadata.offset,
                }
            },
        )

    except Exception as exc:

        logger.critical(
            "Failed to send event to DLQ",
            extra={
                "event_data": {
                    "error": str(exc),

                    "topic": message.topic,

                    "partition": message.partition,

                    "offset": message.offset,
                }
            },
        )

        # Very important:
        # If DLQ publishing fails, we don't want
        # to commit the Kafka offset.
        raise


# ============================================================
# COMMIT BATCH
# ============================================================

def commit_batch(messages):

    """
    Manually commit offsets.

    Kafka expects the NEXT offset to be committed.
    """

    offsets = {}

    for message in messages:

        tp = TopicPartition(
            message.topic,
            message.partition,
        )

        offsets[tp] = OffsetAndMetadata(
            message.offset + 1,
            None,
        )

    consumer.commit(
        offsets=offsets
    )

    logger.info(
        "Batch successfully processed and committed"
    )


# ============================================================
# MAIN CONSUMER LOOP
# ============================================================

def main():

    logger.info(
        f"Consumer starting | "
        f"instance={CONSUMER_INSTANCE} | "
        f"group={CONSUMER_GROUP} | "
        f"topic={TOPIC}"
    )

    try:

        while not shutdown_requested:

            # ------------------------------------------------
            # POLL
            # ------------------------------------------------

            records = consumer.poll(

                timeout_ms=POLL_TIMEOUT_MS,

                max_records=BATCH_SIZE,
            )

            if not records:

                continue

            # Flatten partitions into one list
            messages = [

                message

                for partition_messages
                in records.values()

                for message
                in partition_messages

            ]

            logger.info(
                f"Received batch | messages={len(messages)}"
            )

            # ------------------------------------------------
            # PROCESS
            # ------------------------------------------------

            try:

                success = process_batch(
                    messages
                )

                # ------------------------------------------------
                # COMMIT
                # ------------------------------------------------

                if success:

                    commit_batch(
                        messages
                    )

            except Exception as exc:

                logger.error(
                    "Batch processing failed; offsets NOT committed",
                    extra={
                        "event_data": {
                            "error": str(exc),
                        }
                    },
                )

                # Don't commit.
                #
                # Kafka will redeliver these messages
                # after the consumer resumes/rebalances.

                time.sleep(2)

    finally:

        logger.info(
            "Shutting down consumer"
        )

        try:

            consumer.close()

        finally:

            dlq_producer.flush()

            dlq_producer.close()

        logger.info(
            "Consumer stopped"
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()