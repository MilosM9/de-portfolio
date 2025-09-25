# Koristimo specifičnu verziju Airflow-a koju želimo da koristimo kao osnovu.
FROM apache/airflow:2.9.3

# Instalacija Jave ostaje ista
USER root
RUN apt-get update && apt-get install -y default-jre
USER airflow

# --- Početak izmene ---
# Instaliranje provajdera uz ograničavanje verzija kako bi se sprečili konflikti:
# 1. Ograničavamo Airflow na tačnu verziju slike (2.9.3) da sprečimo nadogradnju na 3.x.
# 2. Ograničavamo protobuf na verziju < 5.0.0 da rešimo konflikt koji prijavljuje pip.
RUN pip install --no-cache-dir \
    "apache-airflow==2.9.3" \
    "protobuf<5.0.0" \
    apache-airflow-providers-apache-spark \
    boto3 \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    requests
# --- Kraj izmene ---