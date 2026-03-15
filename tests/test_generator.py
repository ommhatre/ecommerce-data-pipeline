import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
))

from event_generator.generator import (
    generate_event,
    generate_batch,
    PRODUCT_CATALOGUE,
    EVENT_TYPES,
)


class TestEventStructure:
    """Tests that every generated event has the correct structure."""

    def test_event_has_all_required_fields(self):
        event = generate_event()
        required_fields = [
            "event_id", "user_id", "event_type", "product_id",
            "product_name", "category", "price",
            "quantity", "revenue", "timestamp"
        ]
        for field in required_fields:
            assert field in event, f"Missing field: {field}"

    def test_event_type_is_valid(self):
        for _ in range(50):
            event = generate_event()
            assert event["event_type"] in EVENT_TYPES

    def test_user_id_in_valid_range(self):
        for _ in range(50):
            event = generate_event()
            assert 1 <= event["user_id"] <= 500

    def test_product_exists_in_catalogue(self):
        catalogue_ids = {p["product_id"] for p in PRODUCT_CATALOGUE}
        for _ in range(50):
            event = generate_event()
            assert event["product_id"] in catalogue_ids

    def test_price_is_positive(self):
        for _ in range(50):
            event = generate_event()
            assert event["price"] > 0

    def test_revenue_equals_price_times_quantity(self):
        for _ in range(50):
            event = generate_event()
            expected = round(event["price"] * event["quantity"], 2)
            assert event["revenue"] == expected

    def test_quantity_is_at_least_one(self):
        for _ in range(50):
            event = generate_event()
            assert event["quantity"] >= 1

    def test_event_id_is_unique(self):
        events = generate_batch(100)
        ids = [e["event_id"] for e in events]
        assert len(ids) == len(set(ids)), "Duplicate event IDs found!"

    def test_batch_generates_correct_count(self):
        for size in [1, 10, 50]:
            batch = generate_batch(size)
            assert len(batch) == size