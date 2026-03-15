import os

required_folders = [
    "docker", "event_generator", "kafka_producer",
    "kafka_consumer", "etl", "warehouse",
    "airflow/dags", "dashboards", "tests", "docs"
]

required_files = [
    "requirements.txt", ".env", ".gitignore",
    "README.md", "docker-compose.yml",
    "event_generator/generator.py",
    "kafka_producer/producer.py",
    "kafka_consumer/consumer.py",
    "etl/transform.py",
    "warehouse/schema.sql",
]

print("🔍 Checking folder structure...\n")
all_good = True

for folder in required_folders:
    if os.path.isdir(folder):
        print(f"  ✅ {folder}/")
    else:
        print(f"  ❌ MISSING: {folder}/")
        all_good = False

print("\n🔍 Checking required files...\n")

for filepath in required_files:
    if os.path.isfile(filepath):
        print(f"  ✅ {filepath}")
    else:
        print(f"  ❌ MISSING: {filepath}")
        all_good = False

print("\n🔍 Checking Python packages...\n")

packages = [
    "kafka", "psycopg2", "sqlalchemy",
    "pandas", "dotenv", "faker"
]

for pkg in packages:
    try:
        __import__(pkg)
        print(f"  ✅ {pkg}")
    except ImportError:
        print(f"  ❌ NOT INSTALLED: {pkg}")
        all_good = False

print()
if all_good:
    print("🎉 Everything is set up correctly! Ready for Phase 3.")
else:
    print("⚠️  Some issues found. Fix them before moving to Phase 3.")