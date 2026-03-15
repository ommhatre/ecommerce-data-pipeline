# Architecture - Phase 1
# System Architecture

## High-Level Data Flow

\```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                                 │
│              Python Event Generator                              │
│         (Simulates e-commerce user activity)                     │
└─────────────────────────┬───────────────────────────────────────┘
                          │ JSON Events
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                   MESSAGE BROKER                                 │
│                   Apache Kafka                                   │
│              Topic: ecommerce_events                             │
│         Decouples producers from consumers                       │
└─────────────────────────┬───────────────────────────────────────┘
                          │ Streaming Events
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│              STREAM PROCESSING LAYER                             │
│            PySpark Structured Streaming                          │
│                                                                  │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │ JSON Parser │→ │ ETL Transforms│→ │   JDBC Writer          │ │
│  │             │  │ fact_events   │  │   PostgreSQL           │ │
│  │ Schema      │  │ product_metrics│  │                       │ │
│  │ Validation  │  │ category_metrics│ │                       │ │
│  │             │  │ user_metrics  │  │                       │ │
│  └─────────────┘  └──────────────┘  └────────────────────────┘ │
└─────────────────────────┬───────────────────────────────────────┘
                          │ Transformed Data
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                  DATA WAREHOUSE                                  │
│                    PostgreSQL                                    │
│                                                                  │
│  ┌──────────────┐  ┌─────────────────┐  ┌──────────────────┐  │
│  │ fact_events  │  │ product_metrics  │  │category_metrics  │  │
│  │ (raw events) │  │ (per product)    │  │(per category)    │  │
│  └──────────────┘  └─────────────────┘  └──────────────────┘  │
│                    ┌─────────────────┐                          │
│                    │  user_metrics   │                          │
│                    │  (per user)     │                          │
│                    └─────────────────┘                          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION                                 │
│                   Apache Airflow                                 │
│                                                                  │
│  start → health_checks → init_schema → spark_job →             │
│  quality_checks → summary → end                                 │
│                                                                  │
│  Schedule: @hourly                                               │
└─────────────────────────────────────────────────────────────────┘
\```

## Component Details

| Component | Technology | Purpose |
|---|---|---|
| Event Generator | Python + Faker | Simulates user activity |
| Message Broker | Apache Kafka | Real-time event streaming |
| Stream Processor | PySpark 3.5 | ETL transformations |
| Data Warehouse | PostgreSQL 15 | Persistent storage |
| Orchestrator | Apache Airflow 2.9 | Pipeline scheduling |
| Containerisation | Docker Compose | Infrastructure management |