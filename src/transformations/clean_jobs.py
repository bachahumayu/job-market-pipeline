from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit
from pathlib import Path
from datetime import datetime

# ---------- Spark Session ----------
spark = SparkSession.builder \
    .appName("JobMarketPipeline-Silver") \
    .master("local[*]") \
    .getOrCreate()

# ---------- Paths ----------
RAW_PATH = Path("data/raw")
CLEAN_PATH = Path("data/clean")
CLEAN_PATH.mkdir(parents=True, exist_ok=True)

# ---------- Read Latest Adzuna JSON ----------
latest_file = sorted(RAW_PATH.glob("jobs_raw_*.json"))[-1]
df = spark.read.option("multiline", True).json(str(latest_file))

# ---------- Flatten 'data' field ----------
df = df.select(explode(col("results")).alias("job")).select("job.*")

# ---------- Keep Important Columns ----------
columns_to_keep = {
    "title": "title",
    "company": "company",
    "location": "location",
    "salary_min": "salary_min",
    "salary_max": "salary_max",
    "created": "created_at",
    "redirect_url": "url",
    "id": "job_id"
}

# Add missing columns if not present
for old, new in columns_to_keep.items():
    if old not in df.columns:
        df = df.withColumn(old, lit(None))

df = df.select([col(old).alias(new) for old, new in columns_to_keep.items()])

# ---------- Remove Duplicates ----------
df = df.dropDuplicates(["job_id"])

# ---------- Add ingestion date ----------
df = df.withColumn("ingestion_date", lit(datetime.now().strftime("%Y-%m-%d")))

# ---------- Write to Silver Layer ----------
output_file = CLEAN_PATH / f"jobs_clean_{datetime.now().strftime('%Y-%m-%d')}.parquet"
df.write.mode("overwrite").parquet(str(output_file))

print(f"Cleaned jobs saved to {output_file}")

spark.stop()
