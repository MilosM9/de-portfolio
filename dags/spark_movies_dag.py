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
    """
    Ovaj DAG orkestrira Spark posao koji obrađuje podatke o filmovima.
    """

    run_spark_job = SparkSubmitOperator(
        task_id="run_spark_job",
        conn_id="spark_connection",
        application="/opt/airflow/dags/spark_job.py",
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
            # Standardna S3 konfiguracija
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            # Forsiramo samo problematičnu timeout opciju preko Java sistemskih propertija
            "spark.driver.extraJavaOptions": "-Dfs.s3a.connection.timeout=60000",
            "spark.executor.extraJavaOptions": "-Dfs.s3a.connection.timeout=60000",
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.2"
    )

spark_movie_processing_pipeline()