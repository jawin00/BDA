"""Pipeline step 6: write enriched + clustered events to MongoDB and Cassandra.

Reads:  hdfs:///processed/events_clustered/   (Parquet)
        hdfs:///mr_out/keyword_freq/          (TSV from MapReduce)
Writes:
  MongoDB:  disasters.events            — one document per event (random access)
  MongoDB:  disasters.keyword_freq      — MR results, for the dashboard MR page
  Cassandra: disasters.events_by_region_date  — time-series by (country, date)

Why two NoSQL stores? Different access patterns. Mongo for free-form
filter/paginate by event_type/severity/country; Cassandra for fast
time-series scans per region (partition by (country, date_bucket), cluster
by event_time desc).

Syllabus tags exercised: NoSQL, MongoDB, Apache Cassandra, Spark SQL —
Loading and Saving Data.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

EVENTS_IN = "hdfs://localhost:9000/processed/events_clustered"
MR_IN = "hdfs://localhost:9000/mr_out/keyword_freq"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")


def ensure_cassandra_schema():
    from cassandra.cluster import Cluster
    cluster = Cluster([CASSANDRA_HOST])
    sess = cluster.connect()
    sess.execute("""
        CREATE KEYSPACE IF NOT EXISTS disasters
          WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    sess.execute("""
        CREATE TABLE IF NOT EXISTS disasters.events_by_region_date (
            country     text,
            date_bucket text,
            event_time  timestamp,
            event_id    text,
            source      text,
            event_type  text,
            severity    float,
            cluster_id  int,
            title       text,
            lat         double,
            lon         double,
            PRIMARY KEY ((country, date_bucket), event_time, event_id)
        ) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC)
    """)
    cluster.shutdown()
    print("[06] Cassandra keyspace + table ready")


def main():
    spark = (SparkSession.builder
             .appName("06_load_serving_stores")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.mongodb.write.connection.uri", MONGO_URI)
             .config("spark.cassandra.connection.host", CASSANDRA_HOST)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    ensure_cassandra_schema()

    events = spark.read.option("recursiveFileLookup", "true").parquet(EVENTS_IN)
    n = events.count()
    print(f"[06] events to write: {n}")

    # ---- Write to MongoDB ----
    print("[06] writing events -> MongoDB.disasters.events")
    mongo_df = events.select(
        F.col("event_id"), F.col("source"), F.col("title"), F.col("text"),
        F.col("lat"), F.col("lon"), F.col("ts"), F.col("country"),
        F.col("event_type"), F.col("classify_conf"),
        F.col("severity"), F.col("cluster_id"),
    )
    (mongo_df.write
        .format("mongodb")
        .option("database", "disasters")
        .option("collection", "events")
        .mode("overwrite")
        .save())

    # MR results (TSV) -> Mongo for the dashboard MR page.
    print("[06] writing MR keyword_freq -> MongoDB.disasters.keyword_freq")
    mr_schema = StructType([
        StructField("year", StringType()),
        StructField("country", StringType()),
        StructField("term", StringType()),
        StructField("count", IntegerType()),
    ])
    try:
        mr_df = (spark.read.option("sep", "\t").schema(mr_schema).csv(MR_IN)
                 .where(F.col("year").isNotNull()))
        if mr_df.limit(1).count() > 0:
            (mr_df.write
                .format("mongodb")
                .option("database", "disasters")
                .option("collection", "keyword_freq")
                .mode("overwrite")
                .save())
            print(f"[06] MR rows written: {mr_df.count()}")
    except Exception as exc:
        print(f"[06] MR results not loaded ({exc})", file=sys.stderr)

    # ---- Write to Cassandra ----
    print("[06] writing events -> Cassandra.disasters.events_by_region_date")
    cass_df = (events
               .where(F.col("ts").isNotNull())
               .select(
                   F.coalesce(F.col("country"), F.lit("UNK")).alias("country"),
                   F.date_format("ts", "yyyy-MM").alias("date_bucket"),
                   F.col("ts").alias("event_time"),
                   F.col("event_id"),
                   F.col("source"),
                   F.col("event_type"),
                   F.col("severity").cast("float").alias("severity"),
                   F.col("cluster_id").cast("int").alias("cluster_id"),
                   F.col("title"),
                   F.col("lat"),
                   F.col("lon"),
               ))
    (cass_df.write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "disasters")
        .option("table", "events_by_region_date")
        .option("confirm.truncate", "true")
        .mode("append")
        .save())

    print("[06] done")
    spark.stop()


if __name__ == "__main__":
    main()
