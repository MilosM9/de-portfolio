import boto3

def upload_file_to_s3(file_path, bucket_name, object_name):
    """
    Funkcija koja uploaduje fajl na S3 bucket.
    :param file_path: Putanja do fajla na lokalnom računaru.
    :param bucket_name: Ime S3 bucketa.
    :param object_name: Ime koje će fajl imati na S3.
    """
    s3_client = boto3.client('s3')
    try:
        print(f"Pokušavam da uploadujem fajl '{file_path}' u bucket '{bucket_name}'...")
        s3_client.upload_file(file_path, bucket_name, object_name)
        print("Upload uspešan!")
        return True
    except Exception as e:
        print(f"Došlo je do greške: {e}")
        return False

if __name__ == "__main__":
    # !!! VAŽNO: Zameni ove vrednosti svojim podacima ako je potrebno !!!
    
    # 1. Ime tvog S3 bucketa
    moj_bucket = "milos-de-portfolio-bucket-2025"
    
    # 2. Tačno ime fajla koji se nalazi u tvom de-portfolio folderu
    fajl_za_upload = "IMDB Movies 2000 - 2020.csv" 
    
    # 3. Ime pod kojim želiš da sačuvaš fajl na S3.
    # Možemo ga staviti u "folder" 'raw_data' radi bolje organizacije.
    ime_objekta_na_s3 = f"raw_data/{fajl_za_upload}"

    # Pozivamo funkciju
    upload_file_to_s3(fajl_za_upload, moj_bucket, ime_objekta_na_s3)