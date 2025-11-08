from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("MovieProcessingJob")
    .getOrCreate()
)

print("SparkSession podignut.")

try:
    df = spark.read.csv(
        "s3a://data-lake/IMDB_Movies_2000_2020.csv",
        header=True,
        inferSchema=True,
    )
    print(f"Ucitano {df.count()} redova.")

    cleaned_df = (
        df.filter(col("duration").isNotNull() & (col("duration") > 0))
          .withColumn("duration", col("duration").cast("int"))
    )

    w = Window.partitionBy("year").orderBy(col("duration").desc())
    longest = (
        cleaned_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select("year", "title", "duration")
    )

    output_path = "s3a://data-lake/processed/longest_movie_per_year.csv"
    longest.write.mode("overwrite").csv(output_path, header=True)

    print(f"Snimljeno u {output_path}")

except Exception as e:
    print("Greska tokom Spark posla:", e)
    raise
finally:
    spark.stop()
