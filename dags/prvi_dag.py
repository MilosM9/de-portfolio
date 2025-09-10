from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='nas_prvi_dag',
    start_date=pendulum.datetime(2023, 8, 25, tz="Europe/Belgrade"),
    schedule_interval=None,
    catchup=False,
    tags=['tutorial'],
) as dag:
    # Definišemo prvi zadatak
    zadatak_A = BashOperator(
        task_id='zadatak_A',
        bash_command='echo "Zadatak A je završen."',
    )

    # Definišemo drugi zadatak
    zadatak_B = BashOperator(
        task_id='zadatak_B',
        bash_command='echo "Zadatak B može da počne jer je A uspeo."',
    )

    # !!! KLJUČNI NOVI DEO: Definišemo zavisnost !!!
    # Ovo kažemo Airflow-u: "Prvo izvrši zadatak_A, pa tek onda zadatak_B"
    zadatak_A >> zadatak_B