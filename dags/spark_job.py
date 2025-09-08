from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MinIO to Postgres ETL").getOrCreate()

    # Podešavanja za MinIO
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio123")
    spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Čitanje sa MinIO
    df = spark.read.csv("s3a://imdb/IMDB Movies 2000 - 2020.csv", header=True, inferSchema=True)

    # Transformacija
    filtered_df = df.filter(col("year") > 2018).select("title", "year", "avg_vote")

    # Upis u PostgreSQL
    filtered_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", "top_movies") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    spark.stop()