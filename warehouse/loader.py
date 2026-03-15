import logging
import os
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from pathlib import Path

# Always load .env from the project root regardless of
# where this script is called from
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# DATABASE CONNECTION
# SQLAlchemy is a Python library that talks
# to PostgreSQL for us — we give it a URL
# and it handles the connection details
# ─────────────────────────────────────────

def build_connection_url() -> str:
    """Build the PostgreSQL connection URL from environment variables."""
    host     = os.getenv("POSTGRES_HOST",     "localhost")
    port     = os.getenv("POSTGRES_PORT",     "5432")
    db       = os.getenv("POSTGRES_DB",       "ecommerce")
    user     = os.getenv("POSTGRES_USER",     "admin")
    password = os.getenv("POSTGRES_PASSWORD", "admin123")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def build_engine():
    """
    Create a SQLAlchemy engine — our connection to PostgreSQL.

    The engine is like a phone line to the database.
    We create it once and reuse it for all queries.
    pool_pre_ping=True means it checks the connection
    is still alive before using it.
    """
    url = build_connection_url()
    engine = create_engine(url, pool_pre_ping=True)
    log.info("✅ Database engine created")
    return engine


# ─────────────────────────────────────────
# SCHEMA INITIALISER
# Creates all tables from schema.sql
# ─────────────────────────────────────────

def init_schema(engine) -> None:
    schema_path = os.path.join(
        os.path.dirname(__file__), "schema.sql"
    )
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()

    # BEGIN TRANSACTION — commit happens automatically at the end
    with engine.begin() as conn:
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for statement in statements:
            conn.execute(text(statement))

    log.info("✅ Schema initialised — all tables ready")


# ─────────────────────────────────────────
# LOADERS — one per ETL output table
# ─────────────────────────────────────────

def load_fact_events(df: pd.DataFrame, engine) -> int:
    """
    Insert cleaned events into fact_events table.

    We use INSERT ... ON CONFLICT DO NOTHING so that
    if we accidentally process the same event twice,
    the duplicate is silently ignored.
    This is called IDEMPOTENCY — safe to run twice.

    Returns number of rows inserted.
    """
    if df.empty:
        return 0

    # Prepare dataframe columns to match table schema
    df = df.copy()
    df["processed_at"] = datetime.now(timezone.utc)

    # Convert date/timestamp types
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
    df["timestamp"]  = pd.to_datetime(df["timestamp"])

    cols = [
        "event_id", "user_id", "event_type",
        "product_id", "product_name", "category",
        "price", "quantity", "revenue",
        "event_date", "event_hour",
        "timestamp", "processed_at",
    ]
    df = df[[c for c in cols if c in df.columns]]

    inserted = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            try:
                conn.execute(
                    text("""
                        INSERT INTO fact_events (
                            event_id, user_id, event_type,
                            product_id, product_name, category,
                            price, quantity, revenue,
                            event_date, event_hour,
                            timestamp, processed_at
                        ) VALUES (
                            :event_id, :user_id, :event_type,
                            :product_id, :product_name, :category,
                            :price, :quantity, :revenue,
                            :event_date, :event_hour,
                            :timestamp, :processed_at
                        )
                        ON CONFLICT (event_id) DO NOTHING
                    """),
                    row.to_dict()
                )
                inserted += 1
            except SQLAlchemyError as e:
                log.warning(f"⚠️  Skipped row {row.get('event_id')}: {e}")

    log.info(f"   📥 fact_events      → {inserted} rows inserted")
    return inserted


def load_product_metrics(df: pd.DataFrame, engine) -> int:
    """Insert product metric aggregates."""
    if df.empty:
        return 0

    df = df.copy()
    df["loaded_at"]    = datetime.now(timezone.utc)
    df["window_start"] = pd.to_datetime(df["window_start"])

    cols = [
        "product_id", "product_name", "category",
        "total_views", "total_cart_additions",
        "total_purchases", "total_revenue",
        "window_start", "loaded_at",
    ]
    df = df[[c for c in cols if c in df.columns]]

    with engine.begin() as conn:
        df.to_sql(
            "product_metrics",
            conn,
            if_exists="append",
            index=False,
            method="multi",
        )

    log.info(f"   📥 product_metrics  → {len(df)} rows inserted")
    return len(df)


def load_category_metrics(df: pd.DataFrame, engine) -> int:
    """Insert category metric aggregates."""
    if df.empty:
        return 0

    df = df.copy()
    df["loaded_at"]    = datetime.now(timezone.utc)
    df["window_start"] = pd.to_datetime(df["window_start"])

    cols = [
        "category",
        "total_views", "total_cart_additions",
        "total_purchases", "total_revenue",
        "window_start", "loaded_at",
    ]
    df = df[[c for c in cols if c in df.columns]]

    with engine.begin() as conn:
        df.to_sql(
            "category_metrics",
            conn,
            if_exists="append",
            index=False,
            method="multi",
        )

    log.info(f"   📥 category_metrics → {len(df)} rows inserted")
    return len(df)


def load_user_metrics(df: pd.DataFrame, engine) -> int:
    """Insert user metric aggregates."""
    if df.empty:
        return 0

    df = df.copy()
    df["loaded_at"]  = datetime.now(timezone.utc)
    df["first_seen"] = pd.to_datetime(df["first_seen"], errors="coerce")
    df["last_seen"]  = pd.to_datetime(df["last_seen"],  errors="coerce")

    cols = [
        "user_id", "total_events",
        "total_purchases", "total_spent",
        "first_seen", "last_seen", "loaded_at",
    ]
    df = df[[c for c in cols if c in df.columns]]

    with engine.begin() as conn:
        df.to_sql(
            "user_metrics",
            conn,
            if_exists="append",
            index=False,
            method="multi",
        )

    log.info(f"   📥 user_metrics     → {len(df)} rows inserted")
    return len(df)


# ─────────────────────────────────────────
# MASTER LOADER
# Calls all 4 loaders from one place
# ─────────────────────────────────────────

def load_all(etl_results: dict, engine) -> dict:
    """
    Load all 4 ETL outputs into PostgreSQL.

    Args:
        etl_results: The dict returned by run_etl()
        engine:      SQLAlchemy engine

    Returns:
        Dict of row counts per table
    """
    log.info("💾 Loading ETL results into PostgreSQL...")

    counts = {
        "fact_events":      load_fact_events(
                                etl_results["fact_events"], engine),
        "product_metrics":  load_product_metrics(
                                etl_results["product_metrics"], engine),
        "category_metrics": load_category_metrics(
                                etl_results["category_metrics"], engine),
        "user_metrics":     load_user_metrics(
                                etl_results["user_metrics"], engine),
    }

    total = sum(counts.values())
    log.info(f"✅ Load complete — {total} total rows written to PostgreSQL")
    return counts