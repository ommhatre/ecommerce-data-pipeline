import sys
import os
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
))

from sqlalchemy import text
from warehouse.loader import build_engine, init_schema, load_all
from etl.transform import run_etl

from pathlib import Path
from dotenv import load_dotenv

dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

# ── Sample batch to load ─────────────────────────────────────────
SAMPLE_EVENTS = [
    {"event_id": f"verify-{i}", "user_id": i % 5 + 1,
     "event_type": t, "product_id": i % 25 + 1,
     "product_name": f"Product {i % 25 + 1}",
     "category": c, "price": round(9.99 * (i % 10 + 1), 2),
     "quantity": 1,
     "revenue": round(9.99 * (i % 10 + 1), 2) if t == "purchase" else 0.0,
     "timestamp": "2026-03-08T10:00:00"}
    for i, (t, c) in enumerate([
        ("page_view",   "electronics"),
        ("add_to_cart", "electronics"),
        ("purchase",    "electronics"),
        ("page_view",   "clothing"),
        ("purchase",    "clothing"),
        ("page_view",   "books"),
        ("add_to_cart", "books"),
        ("purchase",    "sports"),
        ("page_view",   "home"),
        ("add_to_cart", "home"),
    ])
]


def verify():
    print("🔍 Verifying PostgreSQL Warehouse...\n")

    # 1. Connect
    print("1️⃣  Connecting to PostgreSQL...")
    engine = build_engine()
    print("   ✅ Connected\n")

    # 2. Init schema
    print("2️⃣  Initialising schema...")
    init_schema(engine)
    print("   ✅ Tables created\n")

    # 3. Run ETL on sample data
    print("3️⃣  Running ETL on sample data...")
    results = run_etl(SAMPLE_EVENTS)
    print("   ✅ ETL complete\n")

    # 4. Load into PostgreSQL
    print("4️⃣  Loading into PostgreSQL...")
    counts = load_all(results, engine)
    print()

    # 5. Query back and verify
    print("5️⃣  Querying tables to verify data landed...\n")

    checks = [
        ("fact_events",      "SELECT COUNT(*) FROM fact_events"),
        ("product_metrics",  "SELECT COUNT(*) FROM product_metrics"),
        ("category_metrics", "SELECT COUNT(*) FROM category_metrics"),
        ("user_metrics",     "SELECT COUNT(*) FROM user_metrics"),
    ]

    all_good = True
    with engine.connect() as conn:
        for table, query in checks:
            row   = conn.execute(text(query)).fetchone()
            count = row[0]
            ok    = count > 0
            icon  = "✅" if ok else "❌"
            print(f"   {icon} {table:20} → {count} rows")
            if not ok:
                all_good = False

        # 6. Sample query — top products by revenue
        print("\n   📊 Top products by revenue:")
        rows = conn.execute(text("""
            SELECT product_name, SUM(revenue) AS total_revenue
            FROM   fact_events
            WHERE  event_type = 'purchase'
            GROUP  BY product_name
            ORDER  BY total_revenue DESC
            LIMIT  5
        """)).fetchall()

        for r in rows:
            print(f"      💰 {r[0]:30} ${r[1]:.2f}")

        # 7. Revenue by category
        print("\n   📊 Revenue by category:")
        rows = conn.execute(text("""
            SELECT category, SUM(total_revenue) AS revenue
            FROM   category_metrics
            GROUP  BY category
            ORDER  BY revenue DESC
        """)).fetchall()

        for r in rows:
            print(f"      🏷️  {r[0]:20} ${r[1]:.2f}")

    print()
    if all_good:
        print("🎉 Warehouse verified! All tables have data. Ready for Phase 9.")
    else:
        print("⚠️  Some tables are empty. Check the output above.")


if __name__ == "__main__":
    verify()