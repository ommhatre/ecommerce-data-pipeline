import json
import time
import logging
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from dotenv import load_dotenv
import sys

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
))

from event_generator.generator import (
    generate_event,
    format_event_for_display
)

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
load_dotenv()

KAFKA_BROKER  = os.getenv("KAFKA_BROKER",  "localhost:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",   "ecommerce_events")

# ─────────────────────────────────────────
# LOGGING SETUP
# Logs tell us what's happening inside
# our app without cluttering the screen
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# SERIALISER
# Converts a Python dict → bytes for Kafka
# ─────────────────────────────────────────
def serialise(data: dict) -> bytes:
    """Convert a Python dictionary to JSON bytes."""
    return json.dumps(data).encode("utf-8")


# ─────────────────────────────────────────
# BUILD PRODUCER
# ─────────────────────────────────────────
def build_producer(retries: int = 5) -> KafkaProducer:
    """
    Create and return a KafkaProducer instance.

    Retries several times in case Kafka is still
    starting up when we first try to connect.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=serialise,
                acks="all",           # Wait for full acknowledgement
                retries=3,            # Retry failed sends up to 3 times
                linger_ms=10,         # Batch messages for 10ms for efficiency
            )
            log.info(f"✅ Connected to Kafka broker at {KAFKA_BROKER}")
            return producer

        except NoBrokersAvailable:
            log.warning(
                f"⚠️  Kafka not ready. "
                f"Attempt {attempt}/{retries}. Retrying in 5 seconds..."
            )
            time.sleep(5)

    raise ConnectionError(
        f"❌ Could not connect to Kafka after {retries} attempts. "
        f"Is Docker running? Check: docker ps"
    )


# ─────────────────────────────────────────
# DELIVERY CALLBACKS
# These fire when Kafka confirms or
# rejects a message we sent
# ─────────────────────────────────────────
def on_send_success(record_metadata):
    """Called when a message is successfully delivered to Kafka."""
    log.debug(
        f"📨 Delivered → topic={record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(exception):
    """Called when a message fails to deliver."""
    log.error(f"❌ Failed to deliver message: {exception}")


# ─────────────────────────────────────────
# PRODUCE EVENTS
# ─────────────────────────────────────────
def produce_events(
    producer:    KafkaProducer,
    topic:       str,
    interval:    float = 1.0,
    max_events:  int   = None,
) -> None:
    """
    Continuously generate and send events to Kafka.

    Args:
        producer:   The KafkaProducer instance
        topic:      Kafka topic name to send to
        interval:   Seconds between each event
        max_events: Stop after this many events (None = run forever)
    """
    total       = 0
    event_counts = {"page_view": 0, "add_to_cart": 0, "purchase": 0}

    log.info(f"🚀 Starting producer → topic: '{topic}'")
    log.info(f"   Interval: {interval}s | Max events: {max_events or 'unlimited'}")
    print("=" * 70)

    try:
        while True:
            if max_events and total >= max_events:
                break

            # 1. Generate a fresh event
            event = generate_event()

            # 2. Send it to Kafka (non-blocking)
            #    The callbacks fire when Kafka responds
            producer.send(topic, value=event) \
                    .add_callback(on_send_success) \
                    .add_errback(on_send_error)

            # 3. Track counts
            event_counts[event["event_type"]] += 1
            total += 1

            # 4. Print to terminal so we can watch live
            print(format_event_for_display(event))

            # 5. Every 20 events print a mini summary
            if total % 20 == 0:
                print("\n" + "─" * 70)
                print(f"📊 Sent to Kafka — Total: {total}")
                print(f"   👁️  Page views:    {event_counts['page_view']}")
                print(f"   🛒 Cart additions: {event_counts['add_to_cart']}")
                print(f"   💰 Purchases:      {event_counts['purchase']}")
                print("─" * 70 + "\n")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n\n🛑 Producer stopped by user.")

    finally:
        # Always flush before exiting
        # flush() forces Kafka to send any buffered messages
        # before we shut down — like hitting "send all" on emails
        log.info("🔄 Flushing remaining messages...")
        producer.flush()
        producer.close()
        log.info(f"✅ Producer closed. Total events sent: {total}")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    producer = build_producer()
    produce_events(
        producer   = producer,
        topic      = KAFKA_TOPIC,
        interval   = 1.0,
        max_events = None,    # Run forever until CTRL+C
    )