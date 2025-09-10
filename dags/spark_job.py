from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("MovieProcessingJob")
    .getOrCreate()
)

df = spark.read.csv("s3a://data-lake/IMDB Movies 2000 - 2020.csv", header=True, inferSchema=True)

cleaned_df = df.filter(col("duration").isNotNull() & (col("duration") > 0)).withColumn(
    "duration", col("duration").cast("integer")
)

windowSpec = Window.partitionBy("year").orderBy(col("duration").desc())

longest_movie_per_year = cleaned_df.withColumn("row_number", row_number().over(windowSpec)).filter(
    col("row_number") == 1
).select("year", "title", "duration")

output_path = "s3a://data-lake/longest_movie_per_year.csv"
longest_movie_per_year.write.mode("overwrite").csv(output_path, header=True)

print("Spark posao uspješno završen. Obrađeni podaci su snimljeni u MinIO.")

spark.stop()