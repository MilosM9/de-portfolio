from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import datetime

@dag(
    dag_id="spark_movie_processing_pipeline",
    schedule_interval=None,
    start_date=datetime(2023, 10, 26),
    catchup=False,
    tags=["spark", "data-processing", "movies"],
)
def spark_movie_processing_pipeline():
    SparkSubmitOperator(
        task_id="run_spark_job",
        conn_id="spark_connection",
        application="/opt/airflow/dags/spark_job.py",
        verbose=True,
        conf={
            "spark.jars": "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.network.timeout": "300s",
        },
    )

spark_movie_processing_pipeline()
