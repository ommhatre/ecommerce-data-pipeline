import os

# All folders to create
folders = [
    "docker",
    "event_generator",
    "kafka_producer",
    "kafka_consumer",
    "etl",
    "warehouse",
    "airflow/dags",
    "dashboards",
    "tests",
    "docs",
]

# Files to create with their content
files = {
    "docker/.gitkeep": "",
    "airflow/dags/.gitkeep": "",
    "dashboards/.gitkeep": "",

    "event_generator/__init__.py": "",
    "event_generator/generator.py": "# Event generator - Phase 4\n",

    "kafka_producer/__init__.py": "",
    "kafka_producer/producer.py": "# Kafka producer - Phase 5\n",

    "kafka_consumer/__init__.py": "",
    "kafka_consumer/consumer.py": "# Kafka consumer - Phase 6\n",

    "etl/__init__.py": "",
    "etl/transform.py": "# ETL transformations - Phase 7\n",

    "warehouse/__init__.py": "",
    "warehouse/schema.sql": "-- PostgreSQL schema - Phase 8\n",

    "tests/__init__.py": "",
    "tests/test_generator.py": "# Tests - Phase 11\n",

    "docs/architecture.md": "# Architecture - Phase 1\n",

    "docker-compose.yml": "# Docker Compose - Phase 3\n",

    "requirements.txt": (
        "kafka-python==2.0.2\n"
        "psycopg2-binary==2.9.9\n"
        "sqlalchemy==2.0.29\n"
        "pandas==2.2.1\n"
        "python-dotenv==1.0.1\n"
        "faker==24.3.0\n"
        "apache-airflow==2.9.0\n"
    ),

    ".env": (
        "# Environment variables\n"
        "KAFKA_BROKER=localhost:9092\n"
        "KAFKA_TOPIC=ecommerce_events\n"
        "POSTGRES_HOST=localhost\n"
        "POSTGRES_PORT=5432\n"
        "POSTGRES_DB=ecommerce\n"
        "POSTGRES_USER=admin\n"
        "POSTGRES_PASSWORD=admin123\n"
    ),

    ".gitignore": (
        "venv/\n"
        "__pycache__/\n"
        "*.pyc\n"
        ".env\n"
        "*.egg-info/\n"
        ".pytest_cache/\n"
    ),

    "README.md": "# E-Commerce Real-Time Data Pipeline\n\nProject under construction.\n",
}

# Create folders
for folder in folders:
    os.makedirs(folder, exist_ok=True)
    print(f"✅ Created folder: {folder}")

# Create files
for filepath, content in files.items():
    with open(filepath, "w") as f:
        f.write(content)
    print(f"✅ Created file: {filepath}")

print("\n🎉 Project structure created successfully!")