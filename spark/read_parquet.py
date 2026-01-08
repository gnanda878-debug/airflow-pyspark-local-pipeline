from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

path = r"D:\files\processed\test3"
df = spark.read.parquet(path)
df.show(truncate=False)
df.printSchema()
