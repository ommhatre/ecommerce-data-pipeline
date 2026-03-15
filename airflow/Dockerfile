FROM apache/airflow:2.9.0

# Install docker CLI so Airflow can run docker exec commands
USER root
RUN apt-get update && \
    apt-get install -y docker.io && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    faker==24.3.0 \
    pandas==2.2.1 \
    psycopg2-binary==2.9.9 \
    python-dotenv==1.0.1 \
    "sqlalchemy==1.4.52" \
    kafka-python==2.1.0