from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestPySpark") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()
