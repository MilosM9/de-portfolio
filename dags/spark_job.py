from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Kreiramo SparkSession. Ako Spark ima master url, koristimo njega.
spark = (
    SparkSession.builder.appName("MovieProcessingJob")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.2")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

# Osiguravamo da su podaci dostupni sa MinIO-a
df = spark.read.csv("s3a://data-lake/IMDB Movies 2000 - 2020.csv", header=True, inferSchema=True)

# Čistimo podatke: Filtriramo filmove bez trajanja i zaokružujemo trajanje
cleaned_df = df.filter(col("duration").isNotNull() & (col("duration") > 0)).withColumn(
    "duration", col("duration").cast("integer")
)

# Kreiramo Window funkciju za rangiranje filmova unutar svake godine
windowSpec = Window.partitionBy("year").orderBy(col("duration").desc())

# Pronalazimo najduži film po godini
longest_movie_per_year = cleaned_df.withColumn("row_number", row_number().over(windowSpec)).filter(
    col("row_number") == 1
).select("year", "title", "duration")

# Snimamo obrađene podatke u novi CSV fajl u MinIO-u
output_path = "s3a://data-lake/longest_movie_per_year.csv"
longest_movie_per_year.write.mode("overwrite").csv(output_path, header=True)

print("Spark posao uspješno završen. Obrađeni podaci su snimljeni u MinIO.")

spark.stop()