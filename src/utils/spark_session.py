from pyspark.sql import SparkSession

def get_spark():
    return (
        SparkSession.builder
        .appName("JobMarketPipeline")
        .master("local[*]")
        .getOrCreate()
    )

if __name__ == "__main__":
    spark = get_spark()
    print(spark.range(5).collect())
    spark.stop()
