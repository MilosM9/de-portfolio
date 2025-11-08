from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("smoke").getOrCreate()
print("=== COUNT ===", spark.range(1, 100001).count())
spark.stop()
