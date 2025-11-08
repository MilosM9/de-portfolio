FROM apache/airflow:2.9.3-python3.11 AS airflow-base

# 1) Root: sistemski paketi + Java
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      curl \
      ca-certificates \
      dos2unix \
  && rm -rf /var/lib/apt/lists/*

# 2) JAVA_HOME + PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 3) Airflow constraints za iste verzije paketa
RUN curl -fsSL -o /tmp/constraints.txt \
    "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"

# 4) Prelaz na airflow user-a
USER airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"

# 5) Provideri + pyspark (spark-submit klijent)
RUN pip install --no-cache-dir -c /tmp/constraints.txt \
      apache-airflow-providers-apache-spark \
      apache-airflow-providers-amazon \
      pyspark==3.5.1

# 6) JAR-ovi za s3a (MinIO/S3)
# koristimo Hadoop AWS + AWS SDK v1 (bez v2, da ne dobijemo NoClassDef za AwsCredentialsProvider iz v2)
USER root
RUN mkdir -p /opt/airflow/jars \
 && curl -fsSL -o /opt/airflow/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o /opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && chown -R airflow:root /opt/airflow/jars

USER airflow
