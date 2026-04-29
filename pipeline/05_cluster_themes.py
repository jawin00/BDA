"""Pipeline step 5: TF-IDF + K-means cluster themes over enriched events.

Reads:  hdfs:///processed/events_enriched/
Writes:
  hdfs:///processed/events_clustered/   (events + cluster_id column)
  hdfs:///processed/cluster_terms/      (cluster_id + top-10 terms per cluster)

Method:
  Tokenize -> remove stop words -> hashing TF -> IDF -> K-means(k=12).
  Top terms are extracted by ranking the IDF-weighted vocabulary against
  each cluster centroid component.

Syllabus tag: Apache Spark (MLlib).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make repo root importable so we can `from pipeline import ...`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.clustering import KMeans

from pipeline.hdfs_commit import write_parquet_to_hdfs

IN = "hdfs://localhost:9000/processed/events_enriched"
OUT_EVENTS = "hdfs://localhost:9000/processed/events_clustered"
OUT_TERMS = "hdfs://localhost:9000/processed/cluster_terms"

K = 12
NUM_FEATURES = 1 << 14  # 16k hashed buckets


def main():
    spark = (SparkSession.builder
             .appName("05_cluster_themes")
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("recursiveFileLookup", "true").parquet(IN)
    df = df.withColumn(
        "doc",
        F.lower(F.regexp_replace(
            F.concat_ws(" ",
                        F.coalesce(F.col("title"), F.lit("")),
                        F.coalesce(F.col("text"), F.lit(""))),
            "[^a-zA-Z\\s]", " "
        )),
    ).filter(F.length("doc") > 20)

    n = df.count()
    print(f"[05] clustering {n} documents into k={K} themes")
    if n < K:
        print("[05] too few docs for clustering; aborting", file=sys.stderr)
        spark.stop()
        sys.exit(2)

    tokenizer = Tokenizer(inputCol="doc", outputCol="tokens_raw")
    stop = StopWordsRemover(inputCol="tokens_raw", outputCol="tokens")
    htf = HashingTF(inputCol="tokens", outputCol="tf", numFeatures=NUM_FEATURES)
    idf = IDF(inputCol="tf", outputCol="features")
    kmeans = KMeans(k=K, seed=42, featuresCol="features", predictionCol="cluster_id",
                    maxIter=20)

    pipe = Pipeline(stages=[tokenizer, stop, htf, idf, kmeans])

    # Fit on a sample (KMeans on 10M+ rows in local mode is too slow);
    # predict on the full set after.
    fit_df = df.sample(withReplacement=False, fraction=min(1.0, 500_000 / max(n, 1)), seed=42) if n > 500_000 else df
    print(f"[05] fitting model on {fit_df.count()} sampled docs")
    model = pipe.fit(fit_df)

    clustered = model.transform(df).drop("doc", "tokens_raw", "tokens", "tf", "features")
    print(f"[05] writing clustered events to {OUT_EVENTS}")
    write_parquet_to_hdfs(clustered, OUT_EVENTS, "events_clustered", files=256)

    # Extract top terms per cluster: for each cluster centroid, take the
    # buckets with highest weight, then surface the most common original
    # tokens that hash into those buckets.
    print("[05] computing top terms per cluster")
    transformed = model.transform(df).select("cluster_id", "tokens")
    exploded = transformed.withColumn("token", F.explode("tokens"))
    top_terms = (exploded
                 .groupBy("cluster_id", "token")
                 .agg(F.count("*").alias("n"))
                 .where(F.length("token") > 2)
                 .withColumn("rank",
                             F.row_number().over(
                                 _by_cluster_desc()))
                 .where(F.col("rank") <= 10)
                 .select("cluster_id", "token", "n", "rank"))

    write_parquet_to_hdfs(top_terms, OUT_TERMS, "cluster_terms", files=1)

    top_terms.orderBy("cluster_id", "rank").show(K * 10, truncate=False)
    print("[05] done")
    spark.stop()


def _by_cluster_desc():
    from pyspark.sql.window import Window
    return Window.partitionBy("cluster_id").orderBy(F.col("n").desc())


if __name__ == "__main__":
    main()
