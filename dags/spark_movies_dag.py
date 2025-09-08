from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import datetime
from os import environ

@dag(
    dag_id="spark_movie_processing_pipeline",
    schedule_interval=None,
    start_date=datetime(2023, 10, 26),
    catchup=False,
    tags=["spark", "data-processing", "movies"],
    doc_md=__doc__,
)
def spark_movie_processing_pipeline():
    """
    Ovaj DAG orkestrira Spark posao koji obrađuje podatke o filmovima.
    Čita CSV datoteku s MinIO-a, pronalazi najduži film po godini,
    i sprema rezultat natrag u MinIO.
    """
    
    # Prvi zadatak: Pokretanje Spark joba
    run_spark_job = SparkSubmitOperator(
        task_id="run_spark_job",
        conn_id="spark_connection",
        application="/opt/airflow/dags/spark_job.py",  # Putanja do skripte
        conf={"spark.master": "spark://spark-master:7077", "spark.driver.host": "airflow-worker"},
        verbose=True,
    )

spark_movie_processing_pipeline()