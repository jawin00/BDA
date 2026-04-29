from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.hdfs_commit import write_parquet_to_hdfs

print("PY_START", flush=True)
spark = (SparkSession.builder
         .appName("smoke_stream_helper")
         .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.driver.host", "127.0.0.1")
         .getOrCreate())
print("SPARK_OK", flush=True)
spark.sparkContext.setLogLevel("WARN")

df = spark.range(25).withColumn("source", F.lit("smoke")).withColumn("title", F.lit("ok"))
print("DF_OK", flush=True)
write_parquet_to_hdfs(df, "hdfs://localhost:9000/processed/commit_smoke_stream", "commit_smoke_stream", files=2)
print("WRITE_OK", flush=True)
spark.stop()
print("STOP_OK", flush=True)
