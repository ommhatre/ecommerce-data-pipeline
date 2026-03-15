import sys
import os
import pytest
import pandas as pd

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
))

from etl.transform import (
    transform_fact_events,
    transform_product_metrics,
    transform_category_metrics,
    transform_user_metrics,
    run_etl,
)

# ─────────────────────────────────────────
# SHARED SAMPLE DATA
# ─────────────────────────────────────────

SAMPLE_EVENTS = [
    {"event_id": "e1", "user_id": 1, "event_type": "page_view",
     "product_id": 1, "product_name": "Laptop",
     "category": "electronics", "price": 1299.99,
     "quantity": 1, "revenue": 0.0,
     "timestamp": "2026-03-08T10:00:00"},

    {"event_id": "e2", "user_id": 1, "event_type": "add_to_cart",
     "product_id": 1, "product_name": "Laptop",
     "category": "electronics", "price": 1299.99,
     "quantity": 1, "revenue": 0.0,
     "timestamp": "2026-03-08T10:01:00"},

    {"event_id": "e3", "user_id": 1, "event_type": "purchase",
     "product_id": 1, "product_name": "Laptop",
     "category": "electronics", "price": 1299.99,
     "quantity": 1, "revenue": 1299.99,
     "timestamp": "2026-03-08T10:02:00"},

    {"event_id": "e4", "user_id": 2, "event_type": "page_view",
     "product_id": 6, "product_name": "Running Shoes",
     "category": "clothing", "price": 89.99,
     "quantity": 1, "revenue": 0.0,
     "timestamp": "2026-03-08T10:03:00"},

    {"event_id": "e5", "user_id": 2, "event_type": "purchase",
     "product_id": 6, "product_name": "Running Shoes",
     "category": "clothing", "price": 89.99,
     "quantity": 2, "revenue": 179.98,
     "timestamp": "2026-03-08T10:04:00"},
]


class TestFactEvents:
    def test_returns_correct_row_count(self):
        df = transform_fact_events(SAMPLE_EVENTS)
        assert len(df) == 5

    def test_has_derived_columns(self):
        df = transform_fact_events(SAMPLE_EVENTS)
        assert "event_date"   in df.columns
        assert "event_hour"   in df.columns
        assert "processed_at" in df.columns

    def test_event_hour_is_correct(self):
        df = transform_fact_events(SAMPLE_EVENTS)
        assert (df["event_hour"] == 10).all()

    def test_empty_input_returns_empty_df(self):
        df = transform_fact_events([])
        assert df.empty

    def test_price_is_float(self):
        df = transform_fact_events(SAMPLE_EVENTS)
        assert df["price"].dtype == float


class TestProductMetrics:
    def test_returns_one_row_per_product(self):
        df = transform_product_metrics(SAMPLE_EVENTS)
        assert len(df) == 2   # Laptop + Running Shoes

    def test_laptop_metrics(self):
        df = transform_product_metrics(SAMPLE_EVENTS)
        laptop = df[df["product_id"] == 1].iloc[0]
        assert laptop["total_views"]          == 1
        assert laptop["total_cart_additions"] == 1
        assert laptop["total_purchases"]      == 1
        assert laptop["total_revenue"]        == 1299.99

    def test_no_purchases_gives_zero_revenue(self):
        no_purchase = [SAMPLE_EVENTS[0]]   # just a page_view
        df = transform_product_metrics(no_purchase)
        assert df.iloc[0]["total_revenue"] == 0.0


class TestCategoryMetrics:
    def test_returns_one_row_per_category(self):
        df = transform_category_metrics(SAMPLE_EVENTS)
        assert len(df) == 2   # electronics + clothing

    def test_electronics_revenue(self):
        df = transform_category_metrics(SAMPLE_EVENTS)
        elec = df[df["category"] == "electronics"].iloc[0]
        assert elec["total_revenue"] == 1299.99

    def test_clothing_purchases(self):
        df = transform_category_metrics(SAMPLE_EVENTS)
        cloth = df[df["category"] == "clothing"].iloc[0]
        assert cloth["total_purchases"] == 1


class TestUserMetrics:
    def test_returns_one_row_per_user(self):
        df = transform_user_metrics(SAMPLE_EVENTS)
        assert len(df) == 2   # user 1 + user 2

    def test_user1_total_events(self):
        df = transform_user_metrics(SAMPLE_EVENTS)
        u1 = df[df["user_id"] == 1].iloc[0]
        assert u1["total_events"]    == 3
        assert u1["total_purchases"] == 1
        assert u1["total_spent"]     == 1299.99

    def test_user2_spent_correctly(self):
        df = transform_user_metrics(SAMPLE_EVENTS)
        u2 = df[df["user_id"] == 2].iloc[0]
        assert u2["total_spent"] == 179.98


class TestRunEtl:
    def test_returns_all_four_keys(self):
        results = run_etl(SAMPLE_EVENTS)
        assert "fact_events"      in results
        assert "product_metrics"  in results
        assert "category_metrics" in results
        assert "user_metrics"     in results

    def test_empty_batch_returns_empty_dfs(self):
        results = run_etl([])
        for df in results.values():
            assert df.empty