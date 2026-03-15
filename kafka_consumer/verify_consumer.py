import json
import sys
import os
import time
import threading

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
))

from kafka import KafkaProducer, KafkaConsumer
from kafka_consumer.consumer import (
    validate_event,
    process_batch,
    deserialise,
)

KAFKA_BROKER  = "localhost:9092"
VERIFY_TOPIC  = "consumer_verify_test"


def test_validator():
    print("🔍 Test 1 — Event Validator\n")

    # Valid event
    valid_event = {
        "event_id":    "abc-123",
        "user_id":     42,
        "event_type":  "purchase",
        "product_id":  7,
        "product_name":"Laptop",
        "category":    "electronics",
        "price":       999.99,
        "quantity":    1,
        "revenue":     999.99,
        "timestamp":   "2026-03-08T10:00:00",
    }

    ok, msg = validate_event(valid_event)
    assert ok, f"Valid event failed validation: {msg}"
    print("  ✅ Valid event passes validation")

    # Missing field
    bad_event = valid_event.copy()
    del bad_event["user_id"]
    ok, msg = validate_event(bad_event)
    assert not ok
    print(f"  ✅ Missing field caught: {msg}")

    # Bad event type
    bad_event2 = valid_event.copy()
    bad_event2["event_type"] = "unknown_action"
    ok, msg = validate_event(bad_event2)
    assert not ok
    print(f"  ✅ Bad event_type caught: {msg}")

    # Negative price
    bad_event3 = valid_event.copy()
    bad_event3["price"] = -50
    ok, msg = validate_event(bad_event3)
    assert not ok
    print(f"  ✅ Negative price caught: {msg}")

    print()


def test_batch_processor():
    print("🔍 Test 2 — Batch Processor\n")

    sample_batch = [
        {"event_type": "page_view",   "revenue": 0,      "user_id": 1,
         "product_name": "Laptop",    "price": 999.99},
        {"event_type": "add_to_cart", "revenue": 0,      "user_id": 2,
         "product_name": "Headphones","price": 149.99},
        {"event_type": "purchase",    "revenue": 149.99, "user_id": 3,
         "product_name": "Headphones","price": 149.99},
        {"event_type": "purchase",    "revenue": 59.99,  "user_id": 4,
         "product_name": "Yoga Mat",  "price": 59.99},
    ]

    summary = process_batch(sample_batch, batch_number=99)

    assert summary["total_events"]   == 4
    assert summary["page_views"]     == 1
    assert summary["cart_additions"] == 1
    assert summary["purchases"]      == 2
    assert summary["total_revenue"]  == 209.98
    print("  ✅ Batch summary counts are correct")
    print("  ✅ Revenue calculation is correct\n")


def test_kafka_roundtrip():
    print("🔍 Test 3 — Kafka End-to-End Roundtrip\n")

    test_messages = [
        {"id": 1, "msg": "hello_kafka"},
        {"id": 2, "msg": "consumer_works"},
        {"id": 3, "msg": "phase_6_complete"},
    ]

    # Send messages
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda d: json.dumps(d).encode("utf-8"),
    )
    for msg in test_messages:
        producer.send(VERIFY_TOPIC, value=msg)
        print(f"  📤 Sent: {msg}")
    producer.flush()
    producer.close()

    # Read them back
    consumer = KafkaConsumer(
        VERIFY_TOPIC,
        bootstrap_servers   = KAFKA_BROKER,
        auto_offset_reset   = "earliest",
        consumer_timeout_ms = 8000,
        value_deserializer  = lambda b: json.loads(b.decode("utf-8")),
        group_id            = "verify_group_phase6",
    )

    received = []
    for message in consumer:
        received.append(message.value)
        print(f"  📥 Received: {message.value}")
        if len(received) >= len(test_messages):
            break
    consumer.close()

    assert len(received) == len(test_messages)
    print(f"\n  ✅ All {len(test_messages)} messages round-tripped successfully\n")


if __name__ == "__main__":
    test_validator()
    test_batch_processor()
    test_kafka_roundtrip()
    print("🎉 All consumer verification tests passed! Ready for Phase 7.")