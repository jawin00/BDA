"""Tiny Spark prep step for the MR job.

The MR job is pure Java/Hadoop and easier to keep simple if its input is
plain TSV: <year>\t<country>\t<text>. This script writes that view into
HDFS at /mr_in/keyword_freq/ from the enriched Parquet.

Why a separate prep step instead of parquet-mr? Parquet input formats need
either an Avro schema embedded in the file or a custom group reader, both of
which add 200+ lines of boilerplate. A 10-line Spark prep is cheaper and
keeps the MR job genuinely simple.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.hdfs_commit import write_text_to_hdfs

IN = "hdfs://localhost:9000/processed/events_enriched"
OUT = "hdfs://localhost:9000/mr_in/keyword_freq"


def main():
    spark = (SparkSession.builder
             .appName("03_stage_mr_input")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("recursiveFileLookup", "true").parquet(IN)

    line = (df.select(
                F.year("ts").cast("string").alias("year"),
                F.coalesce(F.col("country"), F.lit("UNK")).alias("country"),
                F.regexp_replace(
                    F.concat_ws(" ",
                                F.coalesce(F.col("title"), F.lit("")),
                                F.coalesce(F.col("text"), F.lit(""))),
                    "[\\t\\n\\r]", " ",
                ).alias("text"),
            )
            .where(F.col("year").isNotNull())
            .selectExpr("concat_ws('\\t', year, country, text) AS line"))

    print(f"[03/stage] writing {line.count()} lines to {OUT}")
    write_text_to_hdfs(line, OUT, "mr_keyword_freq_input", files=8)
    print("[03/stage] done")
    spark.stop()


if __name__ == "__main__":
    main()
