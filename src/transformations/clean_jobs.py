# src/transformations/clean_jobs.py

from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("Clean Jobs") \
    .getOrCreate()

# Example: create a simple DataFrame
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()

# Example cleaning: filter age > 28
df_clean = df.filter(df.age > 28)
print("Filtered DataFrame (age > 28):")
df_clean.show()

# Stop Spark session
spark.stop()
