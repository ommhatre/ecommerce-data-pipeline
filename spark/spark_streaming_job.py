import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DoubleType, TimestampType,
)

# ─────────────────────────────────────────
# CONFIGURATION
# These are the connection details Spark
# needs to talk to Kafka and PostgreSQL
# ─────────────────────────────────────────

# Inside Docker, Kafka is reachable via its
# container name and internal port 29092
KAFKA_BROKER   = "kafka:29092"
KAFKA_TOPIC    = "ecommerce_events"

# PostgreSQL connection — internal Docker address
POSTGRES_URL   = "jdbc:postgresql://postgres:5432/ecommerce"
POSTGRES_PROPS = {
    "user":     "admin",
    "password": "admin123",
    "driver":   "org.postgresql.Driver",
}

# Where Spark saves its progress checkpoint
# If Spark crashes it resumes from here
CHECKPOINT_DIR = "/opt/spark-apps/checkpoints"

# Path to our JAR files
JARS_DIR = "/opt/spark-jars"


# ─────────────────────────────────────────
# EVENT SCHEMA
# We tell Spark exactly what shape to expect
# when it parses the JSON from Kafka.
#
# Think of this like a template:
# "I expect a JSON with these fields
#  and these data types"
# ─────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",     StringType(),  True),
    StructField("user_id",      IntegerType(), True),
    StructField("event_type",   StringType(),  True),
    StructField("product_id",   IntegerType(), True),
    StructField("product_name", StringType(),  True),
    StructField("category",     StringType(),  True),
    StructField("price",        DoubleType(),  True),
    StructField("quantity",     IntegerType(), True),
    StructField("revenue",      DoubleType(),  True),
    StructField("timestamp",    StringType(),  True),
])


# ─────────────────────────────────────────
# BUILD SPARK SESSION
# SparkSession is the entry point to Spark.
# Think of it like psycopg2.connect() —
# it's how we get a handle to Spark.
# ─────────────────────────────────────────
def build_spark_session() -> SparkSession:
    jar_files = [
        f"{JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.0.jar",
        f"{JARS_DIR}/kafka-clients-3.4.0.jar",
        f"{JARS_DIR}/postgresql-42.7.3.jar",
        f"{JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
        f"{JARS_DIR}/commons-pool2-2.11.1.jar",
    ]
    jars = ",".join(jar_files)

    spark = (
        SparkSession.builder
        .appName("EcommerceStreamingPipeline")
        .master("local[2]")
        .config("spark.jars", jars)
        .config("spark.executor.memory",            "512m")
        .config("spark.driver.memory",              "512m")
        .config("spark.sql.shuffle.partitions",     "2")
        .config("spark.ui.enabled",                 "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("✅ SparkSession created successfully")
    print(f"   App name : {spark.conf.get('spark.app.name')}")
    print(f"   Master   : local[2] (development mode)")

    return spark


# ─────────────────────────────────────────
# READ FROM KAFKA
# This creates a streaming DataFrame.
# It does NOT read data immediately —
# it sets up the pipeline definition.
# Data flows when we call .start()
# ─────────────────────────────────────────
def read_from_kafka(spark: SparkSession):
    """
    Create a streaming DataFrame that reads
    from the Kafka topic.

    Each row in this DataFrame represents
    one Kafka message with these columns:
    - key:       message key (bytes)
    - value:     message body (bytes) ← our JSON is here
    - topic:     which topic it came from
    - partition: which partition
    - offset:    position in the partition
    - timestamp: when Kafka received it
    """
    print(f"📡 Connecting to Kafka at {KAFKA_BROKER}")
    print(f"   Topic: {KAFKA_TOPIC}")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        # Read from the beginning of the topic
        # on first run. After that Spark remembers
        # the offset via checkpointing.
        .option("startingOffsets", "latest")
        # Max messages per micro-batch
        .option("maxOffsetsPerTrigger", 100)
        .option("failOnDataLoss", "false")
        .load()
    )

    return kafka_df


# ─────────────────────────────────────────
# PARSE KAFKA MESSAGES
# The raw Kafka message has a 'value' column
# that contains our JSON as bytes.
#
# We need to:
# 1. Cast bytes → string
# 2. Parse JSON string → struct columns
# 3. Select individual fields as columns
# ─────────────────────────────────────────
def parse_events(kafka_df):
    """
    Parse raw Kafka bytes into a typed DataFrame.

    Raw Kafka row looks like:
    | key | value (bytes) | topic | partition | offset |

    After parsing we get:
    | event_id | user_id | event_type | product_id | ... |
    """

    # Step 1: Cast the value column from bytes to string
    # This gives us the raw JSON string
    string_df = kafka_df.select(
        F.col("value").cast("string").alias("json_string"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    # Step 2: Parse the JSON string using our schema
    # from_json() converts a JSON string into a struct
    # Think of it like json.loads() but for Spark
    parsed_df = string_df.select(
        F.from_json(
            F.col("json_string"),
            EVENT_SCHEMA
        ).alias("event"),
        F.col("kafka_timestamp"),
    )

    # Step 3: Flatten the struct into individual columns
    # Without this, all fields are nested under "event.*"
    events_df = parsed_df.select(
        F.col("event.event_id"),
        F.col("event.user_id"),
        F.col("event.event_type"),
        F.col("event.product_id"),
        F.col("event.product_name"),
        F.col("event.category"),
        F.col("event.price"),
        F.col("event.quantity"),
        F.col("event.revenue"),
        # Parse timestamp string into proper timestamp type
        F.to_timestamp(
            F.col("event.timestamp")
        ).alias("event_timestamp"),
        # Add derived columns
        F.to_date(
            F.col("event.timestamp")
        ).alias("event_date"),
        F.hour(
            F.to_timestamp(F.col("event.timestamp"))
        ).alias("event_hour"),
        # When Spark processed this event
        F.current_timestamp().alias("processed_at"),
    )

    # Filter out null rows (malformed JSON)
    clean_df = events_df.filter(
        F.col("event_id").isNotNull() &
        F.col("user_id").isNotNull() &
        F.col("event_type").isin(
            "page_view", "add_to_cart", "purchase"
        )
    )

    return clean_df


# ─────────────────────────────────────────
# WRITE TO CONSOLE (for testing)
# This prints each micro-batch to the
# terminal so we can see events flowing
# ─────────────────────────────────────────
def write_to_console(events_df):
    """
    Write streaming DataFrame to console.
    Used for testing — replaced by PostgreSQL
    writer in Step 5.
    """
    query = (
        events_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        # Trigger every 10 seconds
        .trigger(processingTime="10 seconds")
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_DIR}/console"
        )
        .start()
    )
    return query


# ─────────────────────────────────────────
# POSTGRESQL WRITER
# Writes a Spark DataFrame into a
# PostgreSQL table using JDBC.
#
# JDBC is like a universal plug adapter —
# it lets Spark talk to any database that
# has a JDBC driver (PostgreSQL, MySQL, etc)
# ─────────────────────────────────────────

def write_to_postgres(df, table_name: str, mode: str = "append") -> int:
    """
    Write a Spark DataFrame to a PostgreSQL table.

    Args:
        df:         Spark DataFrame to write
        table_name: Target PostgreSQL table
        mode:       'append' adds rows
                    'overwrite' replaces the table

    Returns:
        Number of rows written
    """
    # Count rows before writing
    # We do this before the write so we can
    # report how many rows were saved
    row_count = df.count()

    if row_count == 0:
        print(f"   ⏭️  {table_name} — 0 rows, skipping write")
        return 0

    try:
        df.write \
          .mode(mode) \
          .jdbc(
              url        = POSTGRES_URL,
              table      = table_name,
              properties = POSTGRES_PROPS,
          )
        print(f"   ✅ {table_name:25} → {row_count:4} rows written")
        return row_count

    except Exception as e:
        print(f"   ❌ Failed to write {table_name}: {e}")
        return 0


# ─────────────────────────────────────────
# BATCH PROCESSOR — UPDATED
# Now runs ETL AND writes to PostgreSQL
# ─────────────────────────────────────────

def process_batch(batch_df, batch_id):
    """
    Process one micro-batch of events.

    For each batch:
    1. Run all 4 ETL transforms
    2. Write each result to PostgreSQL
    3. Print summary

    Args:
        batch_df:  DataFrame containing this batch
        batch_id:  Auto-incrementing batch number
    """
    from spark_etl import run_spark_etl

    print(f"\n{'='*60}")
    print(f"  📦 PROCESSING BATCH #{batch_id}")
    print(f"{'='*60}")

    count = batch_df.count()
    print(f"  Events in batch: {count}")

    if count == 0:
        print("  ⏭️  Empty batch — skipping")
        return

    # ── Step 1: Run ETL transforms ───────────────────────────────
    print(f"\n🔄 Running Spark ETL...")
    results = run_spark_etl(batch_df)

    # ── Step 2: Write each table to PostgreSQL ───────────────────
    print(f"\n💾 Writing to PostgreSQL...")

    total_written = 0

    # fact_events uses 'append' with duplicate protection
    # We handle duplicates by catching errors on the DB side
    # via the ON CONFLICT DO NOTHING constraint on event_id
    total_written += write_to_postgres(
        results["fact_events"],
        "fact_events",
        mode="append",
    )

    # Metric tables always append — each batch adds new rows
    total_written += write_to_postgres(
        results["product_metrics"],
        "product_metrics",
        mode="append",
    )

    total_written += write_to_postgres(
        results["category_metrics"],
        "category_metrics",
        mode="append",
    )

    total_written += write_to_postgres(
        results["user_metrics"],
        "user_metrics",
        mode="append",
    )

    # ── Step 3: Print batch summary ──────────────────────────────
    print(f"\n  ✅ Batch #{batch_id} complete")
    print(f"     Events processed : {count}")
    print(f"     Rows written to DB: {total_written}")
    print(f"{'='*60}\n")


# ─────────────────────────────────────────
# WRITE USING FOREACH BATCH
# Instead of writing to console,
# we now process each batch through ETL
# ─────────────────────────────────────────

def write_with_etl(events_df):
    """
    Use foreachBatch to process each micro-batch
    through our ETL pipeline.

    foreachBatch is more flexible than format("console")
    because we can run any Python code inside it —
    including our ETL transforms and DB writes.
    """
    query = (
        events_df.writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .trigger(processingTime="10 seconds")
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_DIR}/etl"
        )
        .start()
    )
    return query


def main():
    print("\n" + "=" * 60)
    print("  🚀 E-Commerce PySpark Streaming Pipeline")
    print("     With ETL Transformations")
    print("=" * 60 + "\n")

    # 1. Create Spark session
    spark = build_spark_session()

    # 2. Read from Kafka
    kafka_df = read_from_kafka(spark)

    # 3. Parse JSON events
    events_df = parse_events(kafka_df)

    # 4. Process each batch through ETL
    print("\n🔄 Starting streaming with ETL processing...")
    query = write_with_etl(events_df)

    print("✅ Streaming query started")
    print("   ETL results appear every 10 seconds")
    print("   Press CTRL+C to stop\n")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping streaming query...")
        query.stop()
        spark.stop()
        print("✅ Stopped cleanly")


if __name__ == "__main__":
    main()