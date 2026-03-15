from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# TRANSFORM 1 — FACT EVENTS
# Clean and enrich each raw event.
# One row = one user action.
# This is the source of truth table.
# ─────────────────────────────────────────

def transform_fact_events(events_df: DataFrame) -> DataFrame:
    """
    Clean and enrich raw events into a fact table.

    Adds derived columns and ensures correct types.
    This mirrors what our pandas ETL did in Phase 7
    but runs on Spark DataFrames instead.

    Args:
        events_df: Parsed streaming DataFrame from Kafka

    Returns:
        Cleaned DataFrame ready to load into fact_events
    """
    fact_df = events_df.select(
        F.col("event_id"),
        F.col("user_id"),
        # Ensure lowercase and stripped
        F.lower(F.trim(F.col("event_type"))).alias("event_type"),
        F.col("product_id"),
        F.trim(F.col("product_name")).alias("product_name"),
        F.lower(F.trim(F.col("category"))).alias("category"),
        F.round(F.col("price"),   2).alias("price"),
        F.col("quantity"),
        F.round(F.col("revenue"), 2).alias("revenue"),
        F.col("event_date"),
        F.col("event_hour"),
        F.col("event_timestamp").alias("timestamp"),
        F.col("processed_at"),
    ).filter(
        # Drop rows where critical fields are null
        F.col("event_id").isNotNull() &
        F.col("user_id").isNotNull() &
        (F.col("price") > 0)
    )

    return fact_df


# ─────────────────────────────────────────
# TRANSFORM 2 — PRODUCT METRICS
# Aggregate stats per product in this batch.
#
# For each product we calculate:
# - how many times viewed
# - how many times added to cart
# - how many times purchased
# - total revenue generated
# ─────────────────────────────────────────

def transform_product_metrics(events_df: DataFrame) -> DataFrame:
    """
    Aggregate event data into per-product metrics.

    We use a pivot-style approach:
    Count each event_type separately per product,
    then join them all together.

    Args:
        events_df: Parsed streaming DataFrame from Kafka

    Returns:
        DataFrame with one row per product
    """
    # Window start = current hour rounded down
    # This groups metrics into hourly buckets
    window_start = F.date_trunc("hour", F.current_timestamp())

    # ── Views per product ────────────────────────────────────────
    views = (
        events_df
        .filter(F.col("event_type") == "page_view")
        .groupBy("product_id", "product_name", "category")
        .agg(F.count("*").alias("total_views"))
    )

    # ── Cart additions per product ───────────────────────────────
    carts = (
        events_df
        .filter(F.col("event_type") == "add_to_cart")
        .groupBy("product_id", "product_name", "category")
        .agg(F.count("*").alias("total_cart_additions"))
    )

    # ── Purchases + revenue per product ─────────────────────────
    purchases = (
        events_df
        .filter(F.col("event_type") == "purchase")
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.count("*").alias("total_purchases"),
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
        )
    )

    # ── All unique products in this batch ────────────────────────
    all_products = (
        events_df
        .select("product_id", "product_name", "category")
        .distinct()
    )

    # ── Join everything together ─────────────────────────────────
    # Left join so products with no purchases still appear
    result = (
        all_products
        .join(views,     on="product_id", how="left")
        .join(carts,     on="product_id", how="left")
        .join(purchases, on="product_id", how="left")
    )

    # ── Drop duplicate name/category columns from joins ──────────
    result = result.select(
        all_products["product_id"],
        all_products["product_name"],
        all_products["category"],
        F.coalesce(F.col("total_views"),          F.lit(0)).alias("total_views"),
        F.coalesce(F.col("total_cart_additions"), F.lit(0)).alias("total_cart_additions"),
        F.coalesce(F.col("total_purchases"),      F.lit(0)).alias("total_purchases"),
        F.coalesce(F.col("total_revenue"),        F.lit(0.0)).alias("total_revenue"),
        window_start.alias("window_start"),
    )

    return result


# ─────────────────────────────────────────
# TRANSFORM 3 — CATEGORY METRICS
# Roll up all product activity by category.
#
# Answers: "How did Electronics perform
# this hour vs Clothing?"
# ─────────────────────────────────────────

def transform_category_metrics(events_df: DataFrame) -> DataFrame:
    """
    Aggregate event data into per-category metrics.

    Args:
        events_df: Parsed streaming DataFrame from Kafka

    Returns:
        DataFrame with one row per category
    """
    window_start = F.date_trunc("hour", F.current_timestamp())

    # ── Views per category ───────────────────────────────────────
    views = (
        events_df
        .filter(F.col("event_type") == "page_view")
        .groupBy("category")
        .agg(F.count("*").alias("total_views"))
    )

    # ── Cart additions per category ──────────────────────────────
    carts = (
        events_df
        .filter(F.col("event_type") == "add_to_cart")
        .groupBy("category")
        .agg(F.count("*").alias("total_cart_additions"))
    )

    # ── Purchases + revenue per category ────────────────────────
    purchases = (
        events_df
        .filter(F.col("event_type") == "purchase")
        .groupBy("category")
        .agg(
            F.count("*").alias("total_purchases"),
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
        )
    )

    # ── All unique categories in this batch ──────────────────────
    all_cats = events_df.select("category").distinct()

    # ── Join everything ──────────────────────────────────────────
    result = (
        all_cats
        .join(views,     on="category", how="left")
        .join(carts,     on="category", how="left")
        .join(purchases, on="category", how="left")
        .select(
            "category",
            F.coalesce(F.col("total_views"),          F.lit(0)).alias("total_views"),
            F.coalesce(F.col("total_cart_additions"), F.lit(0)).alias("total_cart_additions"),
            F.coalesce(F.col("total_purchases"),      F.lit(0)).alias("total_purchases"),
            F.coalesce(F.col("total_revenue"),        F.lit(0.0)).alias("total_revenue"),
            window_start.alias("window_start"),
        )
    )

    return result


# ─────────────────────────────────────────
# TRANSFORM 4 — USER METRICS
# Activity summary per user in this batch.
#
# Answers: "How active was each user?
# How much did they spend?"
# ─────────────────────────────────────────

def transform_user_metrics(events_df: DataFrame) -> DataFrame:
    """
    Aggregate event data into per-user metrics.

    Args:
        events_df: Parsed streaming DataFrame from Kafka

    Returns:
        DataFrame with one row per user
    """

    # ── Total events per user ────────────────────────────────────
    total_events = (
        events_df
        .groupBy("user_id")
        .agg(F.count("*").alias("total_events"))
    )

    # ── Purchases + spend per user ───────────────────────────────
    purchase_stats = (
        events_df
        .filter(F.col("event_type") == "purchase")
        .groupBy("user_id")
        .agg(
            F.count("*").alias("total_purchases"),
            F.round(F.sum("revenue"), 2).alias("total_spent"),
        )
    )

    # ── First and last seen per user ─────────────────────────────
    time_stats = (
        events_df
        .groupBy("user_id")
        .agg(
            F.min("event_timestamp").alias("first_seen"),
            F.max("event_timestamp").alias("last_seen"),
        )
    )

    # ── Join everything ──────────────────────────────────────────
    result = (
        total_events
        .join(purchase_stats, on="user_id", how="left")
        .join(time_stats,     on="user_id", how="left")
        .select(
            "user_id",
            "total_events",
            F.coalesce(F.col("total_purchases"), F.lit(0)).alias("total_purchases"),
            F.coalesce(F.col("total_spent"),     F.lit(0.0)).alias("total_spent"),
            "first_seen",
            "last_seen",
        )
    )

    return result


# ─────────────────────────────────────────
# MASTER ETL RUNNER
# Runs all 4 transforms on one batch.
# Called once per micro-batch from the
# streaming job.
# ─────────────────────────────────────────

def run_spark_etl(events_df: DataFrame) -> dict:
    """
    Run all 4 ETL transforms on a batch of events.

    This is the single entry point called by the
    streaming job for every micro-batch.

    Args:
        events_df: One micro-batch of parsed events

    Returns:
        Dictionary of transformed DataFrames
    """
    print(f"\n🔄 Running Spark ETL...")

    results = {
        "fact_events":      transform_fact_events(events_df),
        "product_metrics":  transform_product_metrics(events_df),
        "category_metrics": transform_category_metrics(events_df),
        "user_metrics":     transform_user_metrics(events_df),
    }

    # Print row counts for each transform
    for name, df in results.items():
        count = df.count()
        print(f"   ✅ {name:20} → {count:4} rows")

    return results


# ─────────────────────────────────────────
# DISPLAY HELPER
# Prints each DataFrame for testing
# ─────────────────────────────────────────

def display_etl_results(results: dict) -> None:
    """Print all ETL results in readable format."""
    print("\n" + "=" * 70)
    print("  📊 SPARK ETL RESULTS")
    print("=" * 70)

    for name, df in results.items():
        count = df.count()
        print(f"\n📋 {name.upper()} ({count} rows)")
        print("─" * 70)
        if count == 0:
            print("  (empty batch)")
        else:
            df.show(truncate=False)

    print("=" * 70)