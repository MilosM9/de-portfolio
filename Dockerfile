FROM apache/airflow:2.9.2
USER root
RUN apt-get update && apt-get install -y default-jre
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark