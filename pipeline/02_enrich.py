"""Pipeline step 2: enrich the normalized events with UDF-derived columns.

Reads:  hdfs:///processed/events_raw/
Writes: hdfs:///processed/events_enriched/  (Parquet, partitioned by source)

Adds columns:
  event_type      string   (zero-shot DeBERTa, GPU)
  classify_conf   float
  severity        float    (0..1, regex/rule-based)
  geo_lat,geo_lon double   (geoparse fallback when lat/lon missing)
  geo_country     string

Optimization: only run the expensive GPU classifier on rows whose source is
'social', 'reliefweb', or 'gdelt' (free-form text). USGS/EONET/GDACS already
ship with a typed event category — we map those rule-based.

Syllabus tags exercised: Spark SQL UDFs, Using SQL in Applications,
Loading/Saving Data, Performance (broadcast hint, source filter).
"""

from __future__ import annotations

import sys
import os
from pathlib import Path

# Make repo root importable so we can `from udfs import ...`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from pipeline.hdfs_commit import write_parts_to_hdfs
from udfs import classify, severity, geoparse

IN = "hdfs://localhost:9000/processed/events_raw"
OUT = "hdfs://localhost:9000/processed/events_enriched"

CLASSIFY_SOURCES = {"social", "reliefweb", "gdelt"}


def _is_empty(df) -> bool:
    return df.limit(1).count() == 0


def _rule_event_type(source: str, title: str) -> str:
    s = (source or "").lower()
    t = (title or "").lower()
    if s == "usgs":
        return "earthquake"
    if s == "eonet":
        if "wildfire" in t or "fire" in t:
            return "wildfire"
        if "storm" in t or "cyclone" in t or "hurricane" in t or "typhoon" in t:
            return "storm"
        if "flood" in t:
            return "flood"
        if "volcano" in t:
            return "volcano"
        if "drought" in t:
            return "drought"
        if "iceberg" in t or "snow" in t:
            return "other"
        return "other"
    if s == "gdacs":
        # GDACS titles start with "EQ", "TC", "FL", "VO", "DR", "WF"
        for code, label in [
            ("EQ", "earthquake"), ("TC", "storm"), ("FL", "flood"),
            ("VO", "volcano"), ("DR", "drought"), ("WF", "wildfire"),
        ]:
            if t.startswith(code.lower()):
                return label
        return "other"
    return ""


def main():
    spark = (SparkSession.builder
             .appName("02_enrich")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Register UDFs (severity/geoparse for SQL; classify is invoked via DataFrame API).
    severity.register(spark)
    geoparse.register(spark)

    df = spark.read.option("recursiveFileLookup", "true").parquet(IN)
    df.createOrReplaceTempView("events_raw")
    print(f"[02] input rows: {df.count()}")

    # Step A — rule-based event_type (cheap, runs on every row).
    rule_udf = F.udf(_rule_event_type, StringType())
    df = df.withColumn("rule_event_type", rule_udf(F.col("source"), F.col("title")))

    # Step B — split: rows that need GPU classification vs. rows that don't.
    needs_classify = df.filter(F.col("source").isin(*CLASSIFY_SOURCES))
    has_rule = df.filter(~F.col("source").isin(*CLASSIFY_SOURCES))

    print(f"[02] needs GPU classification: {needs_classify.count()}")
    print(f"[02] rule-classified: {has_rule.count()}")

    # GPU pass. On Windows-native demo runs SKIP_GPU=1 avoids pandas UDF work.
    if os.getenv("SKIP_GPU", "0") == "1" or _is_empty(needs_classify):
        classified = needs_classify.withColumn("event_type", F.lit("other")) \
                                   .withColumn("classify_conf", F.lit(0.0).cast("float"))
    else:
        text_for_class = F.coalesce(F.col("text"), F.col("title"))
        classified = needs_classify.withColumn("_class", classify.classify_event_udf(text_for_class)) \
                                   .withColumn("event_type", F.col("_class.event_type")) \
                                   .withColumn("classify_conf", F.col("_class.classify_conf")) \
                                   .drop("_class")

    rule_done = has_rule.withColumn("event_type", F.col("rule_event_type")) \
                        .withColumn("classify_conf", F.lit(1.0).cast("float"))

    enriched = classified.unionByName(rule_done).drop("rule_event_type")

    # Step C — severity + geoparse via Spark SQL (proves UDFs are registered).
    enriched.createOrReplaceTempView("events_typed")
    # Only run geoparse_text on rows that actually need it (missing lat/lon/country).
    # GDELT already has country code + lat/lon for almost every row, so skipping
    # those saves millions of Python UDF calls.
    final_df = spark.sql("""
        SELECT *,
               score_severity(coalesce(text, title)) AS severity,
               CASE
                 WHEN country IS NOT NULL AND lat IS NOT NULL AND lon IS NOT NULL
                 THEN named_struct('geo_lat', CAST(NULL AS DOUBLE),
                                   'geo_lon', CAST(NULL AS DOUBLE),
                                   'geo_country', CAST(NULL AS STRING))
                 ELSE geoparse_text(coalesce(text, title))
               END AS geo
          FROM events_typed
    """).withColumn("geo_lat", F.col("geo.geo_lat")) \
        .withColumn("geo_lon", F.col("geo.geo_lon")) \
        .withColumn("geo_country", F.col("geo.geo_country")) \
        .drop("geo")

    # Coalesce real lat/lon with geoparse fallback.
    final_df = (final_df
                .withColumn("lat", F.coalesce("lat", "geo_lat"))
                .withColumn("lon", F.coalesce("lon", "geo_lon"))
                .withColumn("country", F.coalesce("country", "geo_country")))

    print(f"[02] writing enriched rows to {OUT} as per-source subdirs")

    sources = [r["source"] for r in df.select("source").distinct().collect() if r["source"]]
    parts = {src: final_df.filter(F.col("source") == src) for src in sorted(sources)}
    files_per_key = {"gdelt": 1, "usgs": 2}
    write_parts_to_hdfs(parts, OUT, "events_enriched", files_per_key=files_per_key)

    print("[02] done")
    spark.stop()


if __name__ == "__main__":
    main()
