import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("meditrack_silver_lab_results")
    .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
    .getOrCreate()
)