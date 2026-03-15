import subprocess
import sys
import os


def verify_spark_container():
    print("🔍 Verifying Spark Setup...\n")

    # ── Check containers are running ────────────────────────────
    print("1️⃣  Checking Spark containers...")
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
        capture_output=True, text=True
    )

    containers = result.stdout.strip().split("\n")
    spark_master_running = False
    spark_worker_running = False

    for line in containers:
        if "spark-master" in line:
            spark_master_running = True
            print(f"   ✅ spark-master: {line.split(chr(9))[1]}")
        if "spark-worker" in line:
            spark_worker_running = True
            print(f"   ✅ spark-worker: {line.split(chr(9))[1]}")

    if not spark_master_running:
        print("   ❌ spark-master is NOT running")
    if not spark_worker_running:
        print("   ❌ spark-worker is NOT running")

    # ── Check JAR files ──────────────────────────────────────────
    print("\n2️⃣  Checking JAR files...")
    jar_dir = os.path.join(os.path.dirname(__file__), "jars")
    required_jars = [
        "spark-sql-kafka-0-10_2.12-3.5.0.jar",
        "kafka-clients-3.4.0.jar",
        "postgresql-42.7.3.jar",
        "spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
        "commons-pool2-2.11.1.jar",
    ]

    all_jars_present = True
    for jar in required_jars:
        path = os.path.join(jar_dir, jar)
        if os.path.exists(path):
            size_kb = os.path.getsize(path) // 1024
            print(f"   ✅ {jar} ({size_kb} KB)")
        else:
            print(f"   ❌ MISSING: {jar}")
            all_jars_present = False

    # ── Check Spark can start a session ─────────────────────────
    print("\n3️⃣  Testing Spark session inside container...")
    test_cmd = """
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('verify').getOrCreate()
df = spark.createDataFrame([(1, 'hello'), (2, 'spark')], ['id', 'word'])
df.show()
spark.stop()
print('SPARK_OK')
"""
    result = subprocess.run(
        ["docker", "exec", "spark-master",
         "python3", "-c", test_cmd],
        capture_output=True, text=True, timeout=60
    )

    if "SPARK_OK" in result.stdout or "SPARK_OK" in result.stderr:
        print("   ✅ Spark session created and DataFrame works")
    else:
        print("   ❌ Spark session failed")
        print("   stdout:", result.stdout[-500:] if result.stdout else "empty")
        print("   stderr:", result.stderr[-500:] if result.stderr else "empty")

    # ── Check Spark UI is accessible ────────────────────────────
    print("\n4️⃣  Checking Spark Master UI...")
    import urllib.request
    try:
        urllib.request.urlopen("http://localhost:8081", timeout=5)
        print("   ✅ Spark UI is accessible at http://localhost:8081")
    except Exception:
        print("   ❌ Spark UI not accessible at http://localhost:8081")

    # ── Final result ─────────────────────────────────────────────
    print()
    if spark_master_running and spark_worker_running and all_jars_present:
        print("🎉 Spark environment is ready! Proceed to NEXT STEP.")
    else:
        print("⚠️  Some issues found. Fix them before continuing.")


if __name__ == "__main__":
    verify_spark_container()