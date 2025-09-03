import boto3

def list_s3_buckets():
    """
    Funkcija koja lista sve S3 buckete koristeći Boto3.
    """
    try:
        # 1. Kreiramo S3 'klijenta'
        # Boto3 će automatski pronaći tvoje sačuvane AWS ključeve
        s3_client = boto3.client('s3')

        # 2. Pozivamo API da nam da listu bucketa
        response = s3_client.list_buckets()

        print("Uspešno povučena lista S3 bucketa:")
        for bucket in response['Buckets']:
            # Ime bucketa se nalazi pod ključem 'Name'
            print(f"  - {bucket['Name']}")

    except Exception as e:
        print(f"Došlo je do greške: {e}")

# Pokrećemo funkciju
if __name__ == "__main__":
    list_s3_buckets()