"""Streaming extension — Spark Structured Streaming, file-source.

Watches /work/data/incoming/ (mounted into the spark container) for JSON
events. Each new file gets read, enriched with the same severity UDF as
batch, and appended to MongoDB.disasters.events plus a Parquet sink at
hdfs:///processed/streamed_events/.

Demo move at viva: in another terminal,
  cp data/samples/sample_event.json data/incoming/
Refresh the dashboard within 30 s — new event appears.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

from udfs import severity, geoparse

_base = str(Path(__file__).resolve().parents[2])
INCOMING   = f"file:///{_base}/data/incoming".replace("\\", "/")
CHECKPOINT = f"file:///{_base}/data/streaming_checkpoint".replace("\\", "/")
PARQUET_OUT = "hdfs://localhost:9000/processed/streamed_events"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("source", StringType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("ts", TimestampType()),
    StructField("country", StringType()),
])


def main():
    spark = (SparkSession.builder
             .appName("streaming_file_source")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.mongodb.write.connection.uri", MONGO_URI)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    severity.register(spark)
    geoparse.register(spark)

    raw = (spark.readStream
           .schema(SCHEMA)
           .option("multiLine", "true")
           .option("maxFilesPerTrigger", 5)
           .json(INCOMING))

    enriched = (raw
                .withColumn("event_type", F.lit("streaming"))
                .withColumn("classify_conf", F.lit(0.0).cast("float"))
                .withColumn("severity",
                            F.expr("score_severity(coalesce(text, title))"))
                .withColumn("cluster_id", F.lit(None).cast("int")))

    print(f"[stream] watching {INCOMING}")

    # Sink 1: HDFS parquet (for batch reuse).
    parq_query = (enriched.writeStream
                  .format("parquet")
                  .option("path", PARQUET_OUT)
                  .option("checkpointLocation", f"{CHECKPOINT}/parquet")
                  .outputMode("append")
                  .trigger(processingTime="15 seconds")
                  .start())

    # Sink 2: MongoDB (foreachBatch — connector doesn't support streaming sink directly).
    def write_mongo(batch_df, batch_id):
        if batch_df.limit(1).count() == 0:
            return
        (batch_df.write
            .format("mongodb")
            .option("database", "disasters")
            .option("collection", "events")
            .mode("append")
            .save())

    mongo_query = (enriched.writeStream
                   .foreachBatch(write_mongo)
                   .option("checkpointLocation", f"{CHECKPOINT}/mongo")
                   .outputMode("append")
                   .trigger(processingTime="15 seconds")
                   .start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
