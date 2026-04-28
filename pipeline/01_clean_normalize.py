"""Pipeline step 1: clean + normalize all raw sources to a common schema.

Reads from: hdfs:///raw/{usgs,eonet,reliefweb,gdacs,gdelt,social}/
Writes to:  hdfs:///processed/events_raw/  (Parquet, partitioned by source)

Common schema:
  event_id      string  (per-source unique id)
  source        string  (usgs, eonet, reliefweb, gdacs, gdelt, social)
  title         string
  text          string  (body / description / tweet text — may be empty)
  lat           double  (nullable)
  lon           double  (nullable)
  ts            timestamp
  country       string  (ISO-2, nullable; resolved later if missing)
  raw_json      string  (original record, for traceability)

Syllabus: Spark SQL — Loading and Saving Data, Using in Applications.
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

from pipeline.hdfs_commit import write_parts_to_hdfs

RAW_BASE = "hdfs://localhost:9000/raw"
OUT = "hdfs://localhost:9000/processed/events_raw"

COMMON_COLS = [
    "event_id", "source", "title", "text", "lat", "lon", "ts", "country", "raw_json",
]


def _is_empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0


def _select(df: DataFrame, source: str, **cols) -> DataFrame:
    """Project to common schema; fill missing columns with nulls."""
    out = {c: F.lit(None) for c in COMMON_COLS}
    out.update(cols)
    out["source"] = F.lit(source)
    out["raw_json"] = F.to_json(F.struct(*df.columns))
    # Accept both Column objects and plain string column names
    resolved = {k: (F.col(v) if isinstance(v, str) else v) for k, v in out.items()}
    return df.select(*[resolved[c].alias(c) for c in COMMON_COLS])


def load_usgs(spark: SparkSession) -> DataFrame:
    df = spark.read.option("multiLine", "true").json(f"{RAW_BASE}/usgs")
    if "features" not in df.columns:
        return spark.createDataFrame([], _empty_schema())
    feats = df.select(F.explode("features").alias("f"))
    flat = feats.select(
        F.col("f.id").alias("event_id"),
        F.col("f.properties.title").alias("title"),
        F.col("f.properties.place").alias("text"),
        F.col("f.geometry.coordinates")[1].alias("lat"),
        F.col("f.geometry.coordinates")[0].alias("lon"),
        (F.col("f.properties.time") / 1000).cast(TimestampType()).alias("ts"),
        F.lit(None).cast(StringType()).alias("country"),
    )
    return _select(flat, "usgs",
                   event_id=F.col("event_id"),
                   title=F.col("title"),
                   text=F.col("text"),
                   lat=F.col("lat"),
                   lon=F.col("lon"),
                   ts=F.col("ts"),
                   country=F.col("country"))


def load_eonet(spark: SparkSession) -> DataFrame:
    df = spark.read.option("multiLine", "true").json(f"{RAW_BASE}/eonet")
    if "events" not in df.columns:
        return spark.createDataFrame([], _empty_schema())
    feats = df.select(F.explode("events").alias("e"))
    geo = feats.select(
        F.col("e.id").alias("event_id"),
        F.col("e.title").alias("title"),
        F.col("e.description").alias("text"),
        F.explode("e.geometry").alias("g"),
    )
    flat = geo.select(
        "event_id", "title", "text",
        F.col("g.coordinates")[1].alias("lat"),
        F.col("g.coordinates")[0].alias("lon"),
        F.to_timestamp("g.date").alias("ts"),
        F.lit(None).cast(StringType()).alias("country"),
    )
    # Keep only the first geometry per event_id (chronologically) to avoid duplication.
    flat = flat.dropDuplicates(["event_id"])
    return _select(flat, "eonet",
                   event_id="event_id", title="title", text="text",
                   lat="lat", lon="lon", ts="ts", country="country")


def load_reliefweb(spark: SparkSession) -> DataFrame:
    df = spark.read.option("multiLine", "true").json(f"{RAW_BASE}/reliefweb")
    if "data" not in df.columns:
        return spark.createDataFrame([], _empty_schema())
    feats = df.select(F.explode("data").alias("d"))
    flat = feats.select(
        F.col("d.id").cast(StringType()).alias("event_id"),
        F.col("d.fields.title").alias("title"),
        F.col("d.fields.body-html").alias("text"),
        F.lit(None).cast(DoubleType()).alias("lat"),
        F.lit(None).cast(DoubleType()).alias("lon"),
        F.to_timestamp("d.fields.date.created").alias("ts"),
        F.col("d.fields.primary_country.iso3").alias("country"),
    )
    return _select(flat, "reliefweb",
                   event_id="event_id", title="title", text="text",
                   lat="lat", lon="lon", ts="ts", country="country")


def load_gdacs(spark: SparkSession) -> DataFrame:
    df = spark.read.option("multiLine", "true").json(f"{RAW_BASE}/gdacs")
    if "features" not in df.columns:
        return spark.createDataFrame([], _empty_schema())
    feats = df.select(F.explode("features").alias("f"))
    flat = feats.select(
        F.col("f.properties.eventid").cast(StringType()).alias("event_id"),
        F.col("f.properties.name").alias("title"),
        F.col("f.properties.description").alias("text"),
        F.col("f.geometry.coordinates")[1].cast(DoubleType()).alias("lat"),
        F.col("f.geometry.coordinates")[0].cast(DoubleType()).alias("lon"),
        F.to_timestamp("f.properties.fromdate").alias("ts"),
        F.col("f.properties.iso3").alias("country"),
    )
    flat = flat.dropDuplicates(["event_id"])
    return _select(flat, "gdacs",
                   event_id="event_id", title="title", text="text",
                   lat="lat", lon="lon", ts="ts", country="country")


def load_gdelt(spark: SparkSession) -> DataFrame:
    # GDELT export.csv has 61 tab-separated columns. Schema reference:
    # https://www.gdeltproject.org/data.html#documentation
    df = spark.read.option("sep", "\t").option("header", "false").csv(f"{RAW_BASE}/gdelt")
    if _is_empty(df):
        return spark.createDataFrame([], _empty_schema())
    flat = df.select(
        F.col("_c0").alias("event_id"),
        F.concat_ws(" - ", F.col("_c26"), F.col("_c5"), F.col("_c15")).alias("title"),
        F.col("_c57").alias("text"),  # SOURCEURL
        F.col("_c39").cast(DoubleType()).alias("lat"),  # ActionGeo_Lat
        F.col("_c40").cast(DoubleType()).alias("lon"),  # ActionGeo_Long
        F.to_timestamp(F.col("_c1"), "yyyyMMdd").alias("ts"),
        F.col("_c37").alias("country"),  # ActionGeo_CountryCode
    )
    return _select(flat, "gdelt",
                   event_id="event_id", title="title", text="text",
                   lat="lat", lon="lon", ts="ts", country="country")


def load_social(spark: SparkSession) -> DataFrame:
    # HumAID is TSV per event; extracted into event subdirs — use recursiveFileLookup.
    paths = []
    for sep in ("\t", ","):
        try:
            sub = (spark.read
                   .option("sep", sep)
                   .option("header", "true")
                   .option("recursiveFileLookup", "true")
                   .csv(f"{RAW_BASE}/social/events_set1"))
            if not _is_empty(sub):
                paths.append(sub)
                break
        except Exception:
            continue
    if not paths:
        return spark.createDataFrame([], _empty_schema())

    out_rows = []
    for sub in paths:
        cols = {c.lower(): c for c in sub.columns}
        text_col = cols.get("tweet_text") or cols.get("text") or list(sub.columns)[-1]
        id_col = cols.get("tweet_id") or cols.get("id") or text_col
        event_col = cols.get("event_name") or cols.get("event")
        flat = sub.select(
            F.col(id_col).cast(StringType()).alias("event_id"),
            F.coalesce(F.col(event_col) if event_col else F.lit(""), F.lit("")).alias("title"),
            F.col(text_col).alias("text"),
            F.lit(None).cast(DoubleType()).alias("lat"),
            F.lit(None).cast(DoubleType()).alias("lon"),
            F.current_timestamp().alias("ts"),
            F.lit(None).cast(StringType()).alias("country"),
        )
        out_rows.append(_select(flat, "social",
                                event_id="event_id", title="title", text="text",
                                lat="lat", lon="lon", ts="ts", country="country"))

    out = out_rows[0]
    for o in out_rows[1:]:
        out = out.unionByName(o)
    return out


def _empty_schema() -> StructType:
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("text", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("ts", TimestampType(), True),
        StructField("country", StringType(), True),
        StructField("raw_json", StringType(), True),
    ])


def main():
    spark = (SparkSession.builder
             .appName("01_clean_normalize")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    loaders = [
        ("usgs", load_usgs),
        ("eonet", load_eonet),
        ("reliefweb", load_reliefweb),
        ("gdacs", load_gdacs),
        ("gdelt", load_gdelt),
        ("social", load_social),
    ]

    parts: dict = {}
    seen_rows = 0
    for name, fn in loaders:
        try:
            df = fn(spark)
            df.createOrReplaceTempView(f"raw_{name}")
            cleaned = spark.sql(f"""
                SELECT event_id, source, title, text, lat, lon, ts, country, raw_json
                  FROM raw_{name}
                 WHERE event_id IS NOT NULL
                   AND (title IS NOT NULL OR text IS NOT NULL)
                   AND ts IS NOT NULL
            """)
            n = cleaned.count()
            print(f"[01] {name}: {n} clean rows")
            if n > 0:
                seen_rows += n
                parts[name] = cleaned
        except Exception as exc:
            print(f"[01] {name}: FAILED ({exc})", file=sys.stderr)

    if not parts:
        print("[01] no data — aborting", file=sys.stderr)
        sys.exit(2)

    print(f"[01] writing up to {seen_rows} rows to {OUT} as per-source subdirs")

    write_parts_to_hdfs(parts, OUT, "events_raw")

    print("[01] done")
    spark.stop()


if __name__ == "__main__":
    main()
