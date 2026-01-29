from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
from pathlib import Path
from datetime import datetime

# ---------- Spark Session ----------
spark = SparkSession.builder \
    .appName("JobMarketPipeline-Gold") \
    .master("local[*]") \
    .getOrCreate()

# ---------- Paths ----------
CLEAN_PATH = Path("data/clean")
GOLD_PATH = Path("data/analytics")
GOLD_PATH.mkdir(parents=True, exist_ok=True)

# ---------- Read Latest Clean Parquet ----------
latest_clean = sorted(CLEAN_PATH.glob("jobs_clean_*.parquet"))[-1]
df = spark.read.parquet(str(latest_clean))

# ---------- 1. Jobs per Company ----------
jobs_per_company = (
    df.groupBy("company")
      .agg(count("*").alias("job_count"))
      .orderBy(col("job_count").desc())
)

jobs_per_company.write.mode("overwrite").parquet(
    str(GOLD_PATH / "jobs_per_company.parquet")
)

# ---------- 2. Jobs per Location ----------
jobs_per_location = (
    df.groupBy("location")
      .agg(count("*").alias("job_count"))
      .orderBy(col("job_count").desc())
)

jobs_per_location.write.mode("overwrite").parquet(
    str(GOLD_PATH / "jobs_per_location.parquet")
)

# ---------- 3. Average Salary per Job Title ----------
avg_salary_by_title = (
    df.filter(col("salary_min").isNotNull())
      .groupBy("title")
      .agg(avg("salary_min").alias("avg_salary"))
      .orderBy(col("avg_salary").desc())
)

avg_salary_by_title.write.mode("overwrite").parquet(
    str(GOLD_PATH / "avg_salary_by_title.parquet")
)

print("Gold layer datasets created successfully")

spark.stop()
