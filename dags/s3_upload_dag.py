import pendulum
from airflow.decorators import dag, task

# Važno: Importujemo Boto3, sada ga Airflow "poznaje"
import boto3
from botocore.exceptions import ClientError

# Ime tvog bucketa, stavićemo ga ovde da ga lako menjamo
BUCKET_NAME = "milos-de-portfolio-bucket-2025" 

@dag(
    dag_id='s3_upload_pipeline_v1',
    start_date=pendulum.datetime(2023, 8, 26, tz="Europe/Belgrade"),
    schedule_interval=None,
    catchup=False,
    tags=['aws', 's3', 'project'],
)
def s3_upload_dag():

    @task
    def create_and_upload_file_to_s3():
        """
        Ovaj zadatak kreira mali tekstualni fajl unutar kontejnera
        i zatim ga uploaduje na S3.
        """
        file_content = f"Test fajl iz Airflow-a, kreiran {pendulum.now()}."
        file_name = "test_iz_airflowa.txt"
        
        # Kreiramo fajl privremeno unutar Docker kontejnera
        with open(f"/tmp/{file_name}", "w") as f:
            f.write(file_content)
        
        print(f"Fajl '{file_name}' kreiran u kontejneru.")

        # Koristimo Boto3 da ga uploadujemo
        s3_client = boto3.client('s3')
        try:
            print(f"Uploadujem fajl u bucket '{BUCKET_NAME}'...")
            s3_client.upload_file(f"/tmp/{file_name}", BUCKET_NAME, file_name)
            print("Upload uspešan!")
        except ClientError as e:
            print(f"Došlo je do greške: {e}")
            raise e # Ovo će oboriti Airflow zadatak ako upload ne uspe
    
    # Pozivamo našu task funkciju da bi se registrovala u DAG-u
    create_and_upload_file_to_s3()

# Instanciramo DAG
s3_upload_dag()