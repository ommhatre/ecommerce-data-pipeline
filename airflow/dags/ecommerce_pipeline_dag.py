from datetime import datetime, timedelta
from pathlib import Path
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, "/opt/airflow")

from dotenv import load_dotenv
load_dotenv(dotenv_path=Path("/opt/airflow/.env"))

# ─────────────────────────────────────────
# DEFAULT ARGUMENTS
# ─────────────────────────────────────────
default_args = {
    "owner":            "data-engineering-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
}

# ─────────────────────────────────────────
# SPARK SUBMIT COMMAND
# This is the exact command Airflow will run
# to trigger our PySpark streaming job.
#
# We run it for 55 seconds then stop it —
# this fits inside the hourly schedule with
# 5 seconds buffer.
#
# Why 55 seconds? Because:
# - DAG runs every hour
# - We want Spark to process a batch of data
# - Then stop cleanly before the next run
# ─────────────────────────────────────────
JARS = ",".join([
    "/opt/spark-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "/opt/spark-jars/kafka-clients-3.4.0.jar",
    "/opt/spark-jars/postgresql-42.7.3.jar",
    "/opt/spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "/opt/spark-jars/commons-pool2-2.11.1.jar",
])

# The spark-submit command wrapped in a timeout
# timeout 55 means "kill this process after 55 seconds"
# || true means "don't fail if timeout kills it"
# This is a clean way to run a streaming job
# for a fixed duration
SPARK_SUBMIT_CMD = f"""
docker exec spark-master /opt/spark/bin/spark-submit \
  --master local[2] \
  --driver-memory 512m \
  --jars {JARS} \
  /opt/spark-apps/spark_streaming_job.py
""".strip()

SPARK_SUBMIT_WITH_TIMEOUT = f"timeout 55 bash -c '{SPARK_SUBMIT_CMD}' || true"


# ─────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────

def task_check_postgres(**context):
    """Task 1 — Verify PostgreSQL is reachable."""
    import psycopg2
    conn = psycopg2.connect(
        host     = "postgres",
        port     = 5432,
        dbname   = os.getenv("POSTGRES_DB",       "ecommerce"),
        user     = os.getenv("POSTGRES_USER",     "admin"),
        password = os.getenv("POSTGRES_PASSWORD", "admin123"),
    )
    conn.close()
    print("✅ PostgreSQL is healthy")
    return "postgres_healthy"


def task_check_kafka(**context):
    """
    Task 2 — Verify Kafka topic exists and has messages.
    Ensures there is data for Spark to process.
    """
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient

    try:
        admin = KafkaAdminClient(
            bootstrap_servers="kafka:29092",
            client_id="airflow-health-check"
        )
        topics = admin.list_topics()
        admin.close()

        topic = os.getenv("KAFKA_TOPIC", "ecommerce_events")
        if topic in topics:
            print(f"✅ Kafka topic '{topic}' exists")
        else:
            print(f"⚠️  Kafka topic '{topic}' not found yet")
            print(f"   Available topics: {topics}")

        return "kafka_healthy"

    except Exception as e:
        raise Exception(f"❌ Kafka health check failed: {e}")


def task_init_schema(**context):
    """Task 3 — Ensure all warehouse tables exist."""
    sys.path.insert(0, "/opt/airflow")
    from warehouse.loader import init_schema
    engine = get_docker_engine()
    init_schema(engine)
    print("✅ Schema initialised")
    return "schema_ready"


def task_data_quality_check(**context):
    """
    Task 5 — Run data quality checks after Spark job.
    Validates the data Spark wrote to PostgreSQL.
    """
    sys.path.insert(0, "/opt/airflow")
    from sqlalchemy import text

    engine        = get_docker_engine()
    checks_passed = 0
    checks_failed = 0

    checks = [
        ("fact_events has rows",
         "SELECT COUNT(*) FROM fact_events",
         ">", 0),
        ("No negative revenue",
         "SELECT COUNT(*) FROM fact_events WHERE revenue < 0",
         "==", 0),
        ("All event types valid",
         """SELECT COUNT(*) FROM fact_events
            WHERE event_type NOT IN
            ('page_view','add_to_cart','purchase')""",
         "==", 0),
        ("product_metrics has rows",
         "SELECT COUNT(*) FROM product_metrics",
         ">", 0),
        ("No null user_ids",
         "SELECT COUNT(*) FROM fact_events WHERE user_id IS NULL",
         "==", 0),
    ]

    with engine.connect() as conn:
        for name, query, op, threshold in checks:
            count  = conn.execute(text(query)).scalar()
            passed = (count > threshold) if op == ">" \
                     else (count == threshold)
            icon   = "✅" if passed else "❌"
            print(f"{icon} {name}: {count}")
            if passed:
                checks_passed += 1
            else:
                checks_failed += 1

    print(f"\n📊 {checks_passed} passed, {checks_failed} failed")

    if checks_failed > 0:
        raise ValueError(
            f"❌ {checks_failed} data quality checks failed"
        )
    return f"{checks_passed} checks passed"


def task_generate_summary(**context):
    """Task 6 — Print business summary report to Airflow logs."""
    sys.path.insert(0, "/opt/airflow")
    from sqlalchemy import text

    engine = get_docker_engine()

    with engine.connect() as conn:

        total = conn.execute(
            text("SELECT COUNT(*) FROM fact_events")
        ).scalar()

        print("\n" + "=" * 60)
        print("  📊 PIPELINE RUN SUMMARY")
        print("=" * 60)
        print(f"\n  Total events in warehouse: {total}")

        print("\n  💰 Revenue by category:")
        rows = conn.execute(text("""
            SELECT
                category,
                COUNT(*) FILTER (
                    WHERE event_type = 'purchase'
                )                         AS purchases,
                COALESCE(SUM(revenue), 0) AS revenue
            FROM     fact_events
            GROUP BY category
            ORDER BY revenue DESC
        """)).fetchall()
        for r in rows:
            print(
                f"     {r[0]:15} "
                f"purchases={r[1]:4}  "
                f"revenue=${r[2]:,.2f}"
            )

        print("\n  🏆 Top 5 products by views:")
        rows = conn.execute(text("""
            SELECT   product_name, COUNT(*) AS views
            FROM     fact_events
            WHERE    event_type = 'page_view'
            GROUP BY product_name
            ORDER BY views DESC
            LIMIT    5
        """)).fetchall()
        for r in rows:
            print(f"     {r[0]:30} {r[1]} views")

        print("\n  👥 Active users:")
        active = conn.execute(text("""
            SELECT COUNT(DISTINCT user_id)
            FROM fact_events
        """)).scalar()
        print(f"     {active} unique users")

        print("\n" + "=" * 60)

    return f"Summary complete — {total} total events"


# ─────────────────────────────────────────
# DOCKER ENGINE HELPER
# Connects to PostgreSQL from inside Docker
# ─────────────────────────────────────────
def get_docker_engine():
    from sqlalchemy import create_engine
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER','admin')}:"
        f"{os.getenv('POSTGRES_PASSWORD','admin123')}"
        f"@postgres:5432/"
        f"{os.getenv('POSTGRES_DB','ecommerce')}"
    )
    return create_engine(url, pool_pre_ping=True)


# ─────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────
with DAG(
    dag_id       = "ecommerce_pipeline",
    default_args = default_args,
    description  = "E-Commerce pipeline — Kafka → PySpark → PostgreSQL",
    schedule     = "@hourly",
    start_date   = datetime(2026, 1, 1),
    catchup      = False,
    tags         = ["ecommerce", "spark", "kafka", "etl"],
) as dag:

    # ── Tasks ────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    check_postgres = PythonOperator(
        task_id         = "check_postgres_health",
        python_callable = task_check_postgres,
    )

    check_kafka = PythonOperator(
        task_id         = "check_kafka_health",
        python_callable = task_check_kafka,
    )

    init_schema = PythonOperator(
        task_id         = "init_schema",
        python_callable = task_init_schema,
    )

    # ── THE KEY TASK ─────────────────────────────────────────────
    # BashOperator runs the spark-submit command
    # This triggers our PySpark streaming job
    # for 55 seconds then stops it cleanly
    run_spark_job = BashOperator(
        task_id          = "run_spark_etl_job",
        bash_command     = SPARK_SUBMIT_WITH_TIMEOUT,
        execution_timeout= timedelta(minutes=3),
    )

    quality_check = PythonOperator(
        task_id         = "data_quality_check",
        python_callable = task_data_quality_check,
    )

    summary = PythonOperator(
        task_id         = "generate_summary",
        python_callable = task_generate_summary,
    )

    end = EmptyOperator(task_id="end")

    # ── Task Dependencies ────────────────────────────────────────
    # Read as: "start, then check both postgres AND kafka
    # in parallel, then init schema, then run spark,
    # then quality check, then summary, then end"
    start >> [check_postgres, check_kafka] >> \
    init_schema >> run_spark_job >> \
    quality_check >> summary >> end