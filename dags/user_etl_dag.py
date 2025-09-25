from __future__ import annotations
import pendulum
import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import date

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# =============================================================================
# PRVO DEFINIŠEMO NAŠE PYTHON FUNKCIJE ZA ETL
# =============================================================================

def extract_users():
    """Povlači podatke o korisnicima sa API-ja i vraća ih kao DataFrame."""
    print("Korak E: Izvlačenje podataka sa API-ja...")
    url = 'https://jsonplaceholder.typicode.com/users'
    response = requests.get(url)
    users_df = pd.DataFrame(response.json())
    print("Podaci uspešno povučeni.")
    return users_df

def transform_users(ti):
    """Preuzima DataFrame, transformiše ga i priprema za upis."""
    print("Korak T: Transformacija podataka...")
    users_df = ti.xcom_pull(task_ids='extract_task') # Preuzimamo DataFrame iz prethodnog zadatka
    
    potrebne_kolone = ['id', 'name', 'username', 'email', 'address']
    cleaned_users_df = users_df[potrebne_kolone].copy()
    cleaned_users_df['city'] = cleaned_users_df['address'].apply(lambda x: x['city'])
    cleaned_users_df = cleaned_users_df.drop('address', axis=1) 
    cleaned_users_df['import_date'] = date.today()
    
    print("Podaci uspešno transformisani.")
    return cleaned_users_df

def load_users(ti):
    """Preuzima transformisani DataFrame i upisuje ga u bazu."""
    print("Korak L: Upisivanje podataka u PostgreSQL bazu...")      
    cleaned_users_df = ti.xcom_pull(task_ids='transform_task')
    
    # !!! NE ZABORAVI SVOJU LOZINKU !!!
    connection_string = 'postgresql://postgres:Milos098!@host.docker.internal:5432/postgres'
    engine = create_engine(connection_string)
    
    cleaned_users_df.to_sql(
        'api_users',
        engine,
        if_exists='replace',
        index=False
    )
    print("Podaci uspešno upisani u bazu.")

# =============================================================================
# SADA DEFINIŠEMO NAŠ DAG
# =============================================================================

with DAG(
    dag_id='user_etl_pipeline',
    start_date=pendulum.datetime(2023, 8, 25, tz="Europe/Belgrade"),
    schedule_interval=None,
    catchup=False,
    tags=['project'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_users,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_users,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_users,
    )

    # Definišemo zavisnosti
    extract_task >> transform_task >> load_task