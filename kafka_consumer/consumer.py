import json
import time
import logging
import os
import sys
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
))

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
load_dotenv()

KAFKA_BROKER         = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC          = os.getenv("KAFKA_TOPIC",  "ecommerce_events")
KAFKA_GROUP_ID       = "ecommerce_consumer_group"
BATCH_SIZE           = 10     # Process events in groups of 10
BATCH_TIMEOUT_SECS   = 30     # Or flush every 30 seconds, whichever comes first

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# DESERIALISER
# Converts bytes from Kafka → Python dict
# The reverse of what the producer did
# ─────────────────────────────────────────
def deserialise(data: bytes) -> dict:
    """Convert raw Kafka bytes back into a Python dictionary."""
    return json.loads(data.decode("utf-8"))


# ─────────────────────────────────────────
# EVENT VALIDATOR
# Checks every event has the right shape
# before we try to store it
# ─────────────────────────────────────────
REQUIRED_FIELDS = {
    "event_id", "user_id", "event_type", "product_id",
    "product_name", "category", "price", "quantity",
    "revenue", "timestamp"
}

VALID_EVENT_TYPES = {"page_view", "add_to_cart", "purchase"}


def validate_event(event: dict) -> tuple[bool, str]:
    """
    Validate a single event dictionary.

    Returns:
        (True, "ok")            if valid
        (False, "reason why")   if invalid
    """
    # Check all required fields exist
    missing = REQUIRED_FIELDS - set(event.keys())
    if missing:
        return False, f"Missing fields: {missing}"

    # Check event_type is one we recognise
    if event["event_type"] not in VALID_EVENT_TYPES:
        return False, f"Unknown event_type: {event['event_type']}"

    # Check price is a positive number
    if not isinstance(event["price"], (int, float)) or event["price"] <= 0:
        return False, f"Invalid price: {event['price']}"

    # Check user_id is a positive integer
    if not isinstance(event["user_id"], int) or event["user_id"] <= 0:
        return False, f"Invalid user_id: {event['user_id']}"

    # Check revenue makes sense
    if not isinstance(event["revenue"], (int, float)) or event["revenue"] <= 0:
        return False, f"Invalid revenue: {event['revenue']}"

    return True, "ok"


# ─────────────────────────────────────────
# BATCH PROCESSOR
# This is where we hand events off to ETL
# For now we log them — Phase 7 adds real ETL
# ─────────────────────────────────────────
def process_batch(batch: list, batch_number: int) -> dict:
    """
    Run ETL and load results into PostgreSQL.
    """
    from etl.transform import run_etl, display_etl_results
    from warehouse.loader import build_engine, init_schema, load_all

    # Run all 4 transforms
    results = run_etl(batch)
    display_etl_results(results)

    # Load into PostgreSQL
    engine = build_engine()
    init_schema(engine)
    counts = load_all(results, engine)

    summary = {
        "batch_number":  batch_number,
        "total_events":  len(batch),
        "rows_written":  sum(counts.values()),
        "processed_at":  datetime.utcnow().isoformat(),
    }

    print(f"\n  📦 BATCH #{batch_number} complete | "
          f"Events: {summary['total_events']} | "
          f"Rows written to DB: {summary['rows_written']}\n")

    return summary

# ─────────────────────────────────────────
# BUILD CONSUMER
# ─────────────────────────────────────────
def build_consumer(retries: int = 5) -> KafkaConsumer:
    """
    Create and return a KafkaConsumer instance.

    enable_auto_commit=False means WE decide when
    to commit offsets — after successful processing only.
    """
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers    = KAFKA_BROKER,
                group_id             = KAFKA_GROUP_ID,
                auto_offset_reset    = "earliest",
                enable_auto_commit   = False,
                value_deserializer   = deserialise,
                session_timeout_ms   = 30000,
                heartbeat_interval_ms= 10000,
            )
            log.info(f"✅ Connected to Kafka at {KAFKA_BROKER}")
            log.info(f"   Subscribed to topic : '{KAFKA_TOPIC}'")
            log.info(f"   Consumer group      : '{KAFKA_GROUP_ID}'")
            return consumer

        except NoBrokersAvailable:
            log.warning(
                f"⚠️  Kafka not ready. "
                f"Attempt {attempt}/{retries}. Retrying in 5s..."
            )
            time.sleep(5)

    raise ConnectionError(
        "❌ Could not connect to Kafka. Is Docker running? "
        "Check: docker ps"
    )


# ─────────────────────────────────────────
# MAIN CONSUME LOOP
# ─────────────────────────────────────────
def consume_events(consumer: KafkaConsumer) -> None:
    """
    Main loop — reads messages from Kafka continuously.

    Collects events into a buffer. When the buffer
    reaches BATCH_SIZE or BATCH_TIMEOUT_SECS passes,
    we process the whole batch together.
    """
    buffer          = []          # Collects events before processing
    batch_number    = 0
    total_consumed  = 0
    total_invalid   = 0
    last_flush_time = time.time()

    log.info("🚀 Consumer started. Waiting for messages...\n")

    try:
        for message in consumer:
            event = message.value

            # ── Validate the event ───────────────────────────────
            is_valid, reason = validate_event(event)

            if not is_valid:
                total_invalid += 1
                log.warning(
                    f"⚠️  Invalid event skipped | "
                    f"Reason: {reason} | "
                    f"Event ID: {event.get('event_id', 'unknown')}"
                )
                continue

            # ── Add to buffer ────────────────────────────────────
            buffer.append(event)
            total_consumed += 1

            # ── Print each event as it arrives ───────────────────
            etype = event["event_type"]
            icons = {
                "page_view":   "👁️ ",
                "add_to_cart": "🛒",
                "purchase":    "💰",
            }
            icon = icons.get(etype, "❓")
            print(
                f"{icon} [{etype.upper():12}] "
                f"User {event['user_id']:4} | "
                f"{event['product_name']:30} | "
                f"${event['price']:7.2f}"
            )

            # ── Decide whether to flush the buffer ───────────────
            time_since_flush = time.time() - last_flush_time
            buffer_full      = len(buffer) >= BATCH_SIZE
            timeout_reached  = time_since_flush >= BATCH_TIMEOUT_SECS

            if buffer_full or timeout_reached:
                batch_number += 1
                reason_str    = "buffer full" if buffer_full else "timeout"

                log.info(
                    f"🔄 Flushing batch #{batch_number} "
                    f"({len(buffer)} events — {reason_str})"
                )

                # Process the batch
                process_batch(buffer, batch_number)

                # Commit offsets AFTER successful processing
                consumer.commit()
                log.info(f"✅ Offsets committed for batch #{batch_number}")

                # Reset buffer and timer
                buffer          = []
                last_flush_time = time.time()

    except KeyboardInterrupt:
        print("\n\n🛑 Consumer stopped by user.")

    finally:
        # Process any remaining events in the buffer
        if buffer:
            batch_number += 1
            log.info(f"🔄 Processing final partial batch #{batch_number}...")
            process_batch(buffer, batch_number)
            consumer.commit()

        consumer.close()
        log.info(
            f"✅ Consumer closed | "
            f"Total consumed: {total_consumed} | "
            f"Invalid skipped: {total_invalid}"
        )


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    consumer = build_consumer()
    consume_events(consumer)