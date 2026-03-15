import json
import sys
import os

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
))

from kafka import KafkaProducer, KafkaConsumer
from kafka_producer.producer import build_producer, serialise

KAFKA_BROKER = "localhost:9092"
VERIFY_TOPIC = "producer_verify_test"
TEST_EVENTS  = 5


def verify():
    print("🔍 Verifying Kafka Producer...\n")

    # ── Step 1: Send test messages ──────────────────────────────
    print(f"📤 Sending {TEST_EVENTS} test messages to '{VERIFY_TOPIC}'...")

    producer = build_producer()
    sent = []

    for i in range(TEST_EVENTS):
        msg = {"test_id": i + 1, "content": f"test_message_{i + 1}"}
        producer.send(VERIFY_TOPIC, value=msg)
        sent.append(msg)
        print(f"   ✅ Sent: {msg}")

    producer.flush()
    producer.close()

    # ── Step 2: Read them back ───────────────────────────────────
    print(f"\n📥 Reading messages back from '{VERIFY_TOPIC}'...")

    consumer = KafkaConsumer(
        VERIFY_TOPIC,
        bootstrap_servers  = KAFKA_BROKER,
        auto_offset_reset  = "earliest",   # Read from the very beginning
        consumer_timeout_ms= 8000,         # Stop waiting after 8 seconds
        value_deserializer = lambda b: json.loads(b.decode("utf-8")),
    )

    received = []
    for message in consumer:
        received.append(message.value)
        print(f"   ✅ Received: {message.value}")
        if len(received) >= TEST_EVENTS:
            break

    consumer.close()

    # ── Step 3: Confirm everything matches ───────────────────────
    print()
    if len(received) == TEST_EVENTS:
        print(f"🎉 All {TEST_EVENTS} messages sent and received successfully!")
        print("   Kafka Producer is working correctly.\n")
    else:
        print(f"❌ Expected {TEST_EVENTS} messages, got {len(received)}")
        sys.exit(1)


if __name__ == "__main__":
    verify()