# Real-Time E-Commerce Data Pipeline

A production-style data engineering project that simulates and processes
real-time e-commerce events through a complete streaming data pipeline.

---

## Architecture
```
Python Event Generator
        ↓
Apache Kafka (Message Broker)
        ↓
PySpark Structured Streaming (ETL)
        ↓
PostgreSQL Data Warehouse
        ↓
Apache Airflow (Orchestration)
```

---

## Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Python | 3.12 | Event generation, ETL logic |
| Apache Kafka | 7.5.0 | Real-time message streaming |
| Apache Spark | 3.5.0 | Distributed stream processing |
| PySpark | 3.5.0 | Python API for Spark |
| PostgreSQL | 15 | Data warehouse |
| Apache Airflow | 2.9.0 | Pipeline orchestration |
| Docker | - | Containerisation |

---

## What This Project Does

Users on an e-commerce website generate events — viewing products,
adding to cart, and making purchases. This pipeline:

1. **Generates** realistic e-commerce events using Python
2. **Streams** events through Apache Kafka in real time
3. **Processes** events using PySpark Structured Streaming
4. **Transforms** raw events into 4 analytical tables
5. **Stores** results in a PostgreSQL data warehouse
6. **Orchestrates** the entire pipeline using Apache Airflow

---

## Project Structure
```
ecommerce-pipeline/
├── event_generator/          # Synthetic event generation
│   └── generator.py          # Faker-based user simulator
├── kafka_producer/           # Kafka message producer
│   └── producer.py
├── kafka_consumer/           # Original Python consumer
│   └── consumer.py
├── spark/                    # PySpark streaming layer
│   ├── spark_streaming_job.py # Main Spark application
│   ├── spark_etl.py          # PySpark ETL transforms
│   └── jars/                 # Kafka + JDBC connector JARs
├── etl/                      # Pandas ETL (original version)
│   └── transform.py
├── warehouse/                # PostgreSQL layer
│   ├── schema.sql            # Table definitions
│   ├── loader.py             # Data loading functions
│   └── analytics_queries.sql # Business intelligence queries
├── airflow/
│   ├── Dockerfile            # Custom Airflow image
│   └── dags/
│       └── ecommerce_pipeline_dag.py
├── tests/                    # Unit tests
├── docs/                     # Architecture docs
├── docker-compose.yml        # All services
├── .env.example              # Environment variable template
└── requirements.txt
```

---

## Data Model

The pipeline produces 4 tables in PostgreSQL:

### `fact_events` — Raw event log
Every user action is stored here. This is the source of truth.

| Column | Type | Description |
|---|---|---|
| event_id | VARCHAR | Unique event identifier |
| user_id | INTEGER | User who triggered the event |
| event_type | VARCHAR | page_view / add_to_cart / purchase |
| product_id | INTEGER | Product involved |
| product_name | VARCHAR | Product display name |
| category | VARCHAR | Product category |
| price | NUMERIC | Product price |
| quantity | INTEGER | Units (purchases only) |
| revenue | NUMERIC | Revenue generated |
| event_date | DATE | Date of event |
| event_hour | INTEGER | Hour of event (0-23) |
| timestamp | TIMESTAMPTZ | Full event timestamp |

### `product_metrics` — Per-product aggregates
Views, cart additions, purchases and revenue per product per hour.

### `category_metrics` — Per-category aggregates
Revenue and activity rolled up by product category per hour.

### `user_metrics` — Per-user activity
Total events, purchases and spend per user per batch.

---

## Event Structure
```json
{
  "event_id": "a3f9c2d1-8b4e-4f7a-9c1d-2e5f8a3b6c9d",
  "user_id": 142,
  "event_type": "purchase",
  "product_id": 7,
  "product_name": "Wireless Headphones",
  "category": "electronics",
  "price": 149.99,
  "quantity": 2,
  "revenue": 299.98,
  "timestamp": "2026-03-15T10:22:01"
}
```

Event types are weighted to mirror real behaviour:
- `page_view` — 70% of events
- `add_to_cart` — 20% of events
- `purchase` — 10% of events

---

## Airflow DAG

The pipeline is orchestrated by an hourly Airflow DAG:
```
start
  ├── check_postgres_health
  └── check_kafka_health
          ↓
     init_schema
          ↓
  run_spark_etl_job        (PySpark streaming, ~55s)
          ↓
  data_quality_check
          ↓
   generate_summary
          ↓
         end
```

---

## Setup & Running

### Prerequisites
- Docker Desktop (6GB+ memory recommended)
- Python 3.10+
- Git

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/ecommerce-pipeline.git
cd ecommerce-pipeline
```

### 2. Set up environment
```bash
python -m venv .venv

# Windows
.\.venv\Scripts\Activate.ps1

# Mac/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### 3. Configure environment variables
```bash
cp .env.example .env
# Edit .env with your settings
```

### 4. Start all services
```bash
docker-compose up -d
```

Wait 2-3 minutes for all services to initialise.

### 5. Verify services are running
```bash
docker ps
```

### 6. Run the pipeline

**Terminal 1 — Start event generator:**
```bash
python kafka_producer/producer.py
```

**Terminal 2 — Start PySpark streaming job:**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master local[2] \
  --driver-memory 512m \
  --jars /opt/spark-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,\
/opt/spark-jars/kafka-clients-3.4.0.jar,\
/opt/spark-jars/postgresql-42.7.3.jar,\
/opt/spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,\
/opt/spark-jars/commons-pool2-2.11.1.jar \
  /opt/spark-apps/spark_streaming_job.py
```

### 7. Monitor in Airflow
Open `http://localhost:8080` (admin / admin123)

### 8. Query the warehouse
```bash
docker exec -it postgres psql -U admin -d ecommerce
```
```sql
SELECT event_type, COUNT(*), ROUND(SUM(revenue)::numeric,2)
FROM fact_events
GROUP BY event_type;
```

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin123 |
| Spark Master UI | http://localhost:8081 | - |
| PostgreSQL | localhost:5433 | admin / admin123 |
| Kafka | localhost:9092 | - |

---

## Running Tests
```bash
pytest tests/ -v
```

---

## Key Concepts Demonstrated

- **Real-time streaming** with Kafka and PySpark Structured Streaming
- **Micro-batch processing** with configurable trigger intervals
- **Star schema** data modelling for analytics
- **ETL pipeline** with validation, transformation and loading
- **Workflow orchestration** with Airflow DAGs
- **Containerised infrastructure** with Docker Compose
- **Data quality checks** integrated into the pipeline
- **Idempotent writes** using PostgreSQL conflict handling
- **Checkpoint-based fault tolerance** in Spark streaming