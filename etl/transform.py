import pandas as pd
from datetime import datetime, timezone
import logging

log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# TRANSFORM 1 — FACT EVENTS
# Clean and enrich each raw event
# ─────────────────────────────────────────

def transform_fact_events(events: list) -> pd.DataFrame:
    """
    Clean raw events into a structured fact table.

    Each row = one user action.
    We add derived columns: event_date, event_hour, processed_at.

    Args:
        events: List of raw event dicts from Kafka consumer

    Returns:
        A cleaned pandas DataFrame
    """
    if not events:
        return pd.DataFrame()

    df = pd.DataFrame(events)

    # ── Parse timestamp ──────────────────────────────────────────
    # Convert the ISO string into a real datetime object
    # so we can extract date and hour from it
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # ── Derive new columns ───────────────────────────────────────
    df["event_date"]    = df["timestamp"].dt.date
    df["event_hour"]    = df["timestamp"].dt.hour
    df["processed_at"]  = datetime.now(timezone.utc).isoformat()

    # ── Clean string columns ─────────────────────────────────────
    df["event_type"]    = df["event_type"].str.strip().str.lower()
    df["category"]      = df["category"].str.strip().str.lower()
    df["product_name"]  = df["product_name"].str.strip()

    # ── Enforce correct data types ───────────────────────────────
    df["user_id"]       = df["user_id"].astype(int)
    df["product_id"]    = df["product_id"].astype(int)
    df["price"]         = df["price"].astype(float).round(2)
    df["quantity"]      = df["quantity"].astype(int)
    df["revenue"]       = df["revenue"].astype(float).round(2)

    # ── Select and order final columns ───────────────────────────
    fact_cols = [
        "event_id", "user_id", "event_type",
        "product_id", "product_name", "category",
        "price", "quantity", "revenue",
        "event_date", "event_hour",
        "timestamp", "processed_at",
    ]

    # Only keep columns that exist in the dataframe
    fact_cols = [c for c in fact_cols if c in df.columns]

    return df[fact_cols].reset_index(drop=True)


# ─────────────────────────────────────────
# TRANSFORM 2 — PRODUCT METRICS
# Aggregate stats per product in this batch
# ─────────────────────────────────────────

def transform_product_metrics(events: list) -> pd.DataFrame:
    """
    Aggregate event data into per-product metrics.

    For each product we count:
    - how many times it was viewed
    - how many times it was added to cart
    - how many times it was purchased
    - total revenue it generated

    Args:
        events: List of raw event dicts

    Returns:
        DataFrame with one row per product
    """
    if not events:
        return pd.DataFrame()

    df = pd.DataFrame(events)
    window_start = datetime.now(timezone.utc).replace(
        minute=0, second=0, microsecond=0
    ).isoformat()

    # ── Count each event type per product ────────────────────────
    # We pivot the event_type counts into separate columns

    # Views per product
    views = (
        df[df["event_type"] == "page_view"]
        .groupby(["product_id", "product_name", "category"])
        .size()
        .reset_index(name="total_views")
    )

    # Cart additions per product
    carts = (
        df[df["event_type"] == "add_to_cart"]
        .groupby(["product_id", "product_name", "category"])
        .size()
        .reset_index(name="total_cart_additions")
    )

    # Purchases + revenue per product
    purchases = (
        df[df["event_type"] == "purchase"]
        .groupby(["product_id", "product_name", "category"])
        .agg(
            total_purchases=("event_type", "count"),
            total_revenue  =("revenue",    "sum"),
        )
        .reset_index()
    )
    purchases["total_revenue"] = purchases["total_revenue"].round(2)

    # ── Get all unique products ───────────────────────────────────
    all_products = (
        df[["product_id", "product_name", "category"]]
        .drop_duplicates()
    )

    # ── Merge everything together ─────────────────────────────────
    result = all_products.copy()

    for frame, col in [
        (views,     "total_views"),
        (carts,     "total_cart_additions"),
        (purchases, ["total_purchases", "total_revenue"]),
    ]:
        merge_cols = (
            ["product_id", "product_name", "category"]
            + (col if isinstance(col, list) else [col])
        )
        result = result.merge(
            frame[["product_id"] + (col if isinstance(col, list) else [col])],
            on="product_id",
            how="left",
        )

    # ── Fill missing values with 0 ────────────────────────────────
    result["total_views"]          = result.get("total_views",          pd.Series(0)).fillna(0).astype(int)
    result["total_cart_additions"] = result.get("total_cart_additions", pd.Series(0)).fillna(0).astype(int)
    result["total_purchases"]      = result.get("total_purchases",      pd.Series(0)).fillna(0).astype(int)
    result["total_revenue"]        = result.get("total_revenue",        pd.Series(0.0)).fillna(0.0).round(2)
    result["window_start"]         = window_start

    return result.reset_index(drop=True)


# ─────────────────────────────────────────
# TRANSFORM 3 — CATEGORY METRICS
# Roll up stats by product category
# ─────────────────────────────────────────

def transform_category_metrics(events: list) -> pd.DataFrame:
    """
    Aggregate event data into per-category metrics.

    Args:
        events: List of raw event dicts

    Returns:
        DataFrame with one row per category
    """
    if not events:
        return pd.DataFrame()

    df          = pd.DataFrame(events)
    window_start = datetime.now(timezone.utc).replace(
        minute=0, second=0, microsecond=0
    ).isoformat()

    # ── Count views per category ──────────────────────────────────
    views = (
        df[df["event_type"] == "page_view"]
        .groupby("category")
        .size()
        .reset_index(name="total_views")
    )

    # ── Count cart additions per category ─────────────────────────
    carts = (
        df[df["event_type"] == "add_to_cart"]
        .groupby("category")
        .size()
        .reset_index(name="total_cart_additions")
    )

    # ── Count purchases + revenue per category ────────────────────
    purchases = (
        df[df["event_type"] == "purchase"]
        .groupby("category")
        .agg(
            total_purchases=("event_type", "count"),
            total_revenue  =("revenue",    "sum"),
        )
        .reset_index()
    )
    purchases["total_revenue"] = purchases["total_revenue"].round(2)

    # ── All unique categories ─────────────────────────────────────
    all_cats = df[["category"]].drop_duplicates()

    # ── Merge ─────────────────────────────────────────────────────
    result = (
        all_cats
        .merge(views,     on="category", how="left")
        .merge(carts,     on="category", how="left")
        .merge(purchases, on="category", how="left")
    )

    result["total_views"]          = result["total_views"].fillna(0).astype(int)
    result["total_cart_additions"] = result["total_cart_additions"].fillna(0).astype(int)
    result["total_purchases"]      = result["total_purchases"].fillna(0).astype(int)
    result["total_revenue"]        = result["total_revenue"].fillna(0.0).round(2)
    result["window_start"]         = window_start

    return result.reset_index(drop=True)


# ─────────────────────────────────────────
# TRANSFORM 4 — USER METRICS
# Activity summary per user
# ─────────────────────────────────────────

def transform_user_metrics(events: list) -> pd.DataFrame:
    """
    Aggregate event data into per-user activity metrics.

    Args:
        events: List of raw event dicts

    Returns:
        DataFrame with one row per user
    """
    if not events:
        return pd.DataFrame()

    df = pd.DataFrame(events)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # ── Total events per user ─────────────────────────────────────
    total_events = (
        df.groupby("user_id")
        .size()
        .reset_index(name="total_events")
    )

    # ── Purchases + spend per user ────────────────────────────────
    purchase_stats = (
        df[df["event_type"] == "purchase"]
        .groupby("user_id")
        .agg(
            total_purchases=("event_type", "count"),
            total_spent    =("revenue",    "sum"),
        )
        .reset_index()
    )
    purchase_stats["total_spent"] = purchase_stats["total_spent"].round(2)

    # ── First and last seen timestamps per user ───────────────────
    time_stats = (
        df.groupby("user_id")
        .agg(
            first_seen=("timestamp", "min"),
            last_seen =("timestamp", "max"),
        )
        .reset_index()
    )

    # ── Merge everything ──────────────────────────────────────────
    result = (
        total_events
        .merge(purchase_stats, on="user_id", how="left")
        .merge(time_stats,     on="user_id", how="left")
    )

    result["total_purchases"] = result["total_purchases"].fillna(0).astype(int)
    result["total_spent"]     = result["total_spent"].fillna(0.0).round(2)
    result["first_seen"]      = result["first_seen"].astype(str)
    result["last_seen"]       = result["last_seen"].astype(str)

    return result.reset_index(drop=True)


# ─────────────────────────────────────────
# MASTER ETL RUNNER
# Runs all 4 transforms on a batch at once
# ─────────────────────────────────────────

def run_etl(events: list) -> dict:
    """
    Run all ETL transforms on a batch of raw events.

    This is the single entry point the consumer calls.
    Returns all 4 transformed DataFrames in a dict.

    Args:
        events: List of raw event dicts from Kafka

    Returns:
        {
            "fact_events":       DataFrame,
            "product_metrics":   DataFrame,
            "category_metrics":  DataFrame,
            "user_metrics":      DataFrame,
        }
    """
    log.info(f"🔄 Running ETL on batch of {len(events)} events...")

    results = {
        "fact_events":      transform_fact_events(events),
        "product_metrics":  transform_product_metrics(events),
        "category_metrics": transform_category_metrics(events),
        "user_metrics":     transform_user_metrics(events),
    }

    for name, df in results.items():
        log.info(f"   ✅ {name:20} → {len(df):4} rows")

    return results


# ─────────────────────────────────────────
# DISPLAY HELPER
# Pretty-prints each DataFrame for testing
# ─────────────────────────────────────────

def display_etl_results(results: dict) -> None:
    """Print all ETL results in a readable format."""
    print("\n" + "=" * 70)
    print("  📊 ETL TRANSFORM RESULTS")
    print("=" * 70)

    for name, df in results.items():
        print(f"\n📋 {name.upper()} ({len(df)} rows)")
        print("─" * 70)
        if df.empty:
            print("  (empty)")
        else:
            print(df.to_string(index=False))
    print("\n" + "=" * 70)