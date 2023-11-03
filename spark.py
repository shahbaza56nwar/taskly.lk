from pyspark.sql import SparkSession

def run_spark_job():
    spark = SparkSession.builder.appName("Taskly.lk Spark App").getOrCreate()
    df = spark.read.csv("data/sample_data.csv", header=True, inferSchema=True)
    summary = df.describe().toPandas()
    spark.stop()
    return summary
