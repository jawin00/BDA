package edu.bda

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Lead-Time Analyzer — the canonical Scala/Spark deliverable.
 *
 * Question: do social mentions tend to precede or follow official alerts
 * for the same disaster, and by how much?
 *
 * Approach:
 *   1. Split events_enriched into "official" (usgs/eonet/gdacs/reliefweb)
 *      and "social" (humaid/social).
 *   2. Self-join on (event_type, country) within a ±48h time window.
 *   3. For each matched pair, compute Δt = social.ts - official.ts (signed
 *      seconds; negative means social was first).
 *   4. Aggregate: per event_type, mean / median / count of Δt.
 *
 * Output: hdfs:///processed/lead_time/{matches,summary}/  (Parquet)
 *
 * Syllabus tags exercised: Scala, Apache Spark, Spark SQL — Linking, Using
 * in Applications, Loading and Saving Data.
 */
object LeadTimeAnalyzer {

  val OFFICIAL_SOURCES = Seq("usgs", "eonet", "gdacs", "reliefweb")
  val SOCIAL_SOURCES   = Seq("social", "gdelt")

  val IN_PATH       = "hdfs://localhost:9000/processed/events_enriched"
  def localUri(path: String): String = "file:///" + path.replace('\\', '/')
  def outPath(name: String, hdfsPath: String): String =
    sys.env.get("BDA_LEAD_TIME_STAGE").map(p => s"${localUri(p)}/$name").getOrElse(hdfsPath)

  val MATCHES_PATH  = outPath("matches", "hdfs://localhost:9000/processed/lead_time/matches")
  val SUMMARY_PATH  = outPath("summary", "hdfs://localhost:9000/processed/lead_time/summary")

  val WINDOW_SECONDS = 48L * 3600L

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("04_lead_time_analyzer")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val all: DataFrame = spark.read.option("recursiveFileLookup", "true").parquet(IN_PATH)
      .filter(col("event_type").isNotNull && col("country").isNotNull)
      .filter(col("event_type") =!= "other")

    val official = all.filter(col("source").isin(OFFICIAL_SOURCES: _*))
      .selectExpr(
        "event_id      as off_id",
        "event_type    as off_type",
        "country       as off_country",
        "ts            as off_ts",
        "severity      as off_sev",
        "source        as off_source"
      )

    val social = all.filter(col("source").isin(SOCIAL_SOURCES: _*))
      .selectExpr(
        "event_id      as soc_id",
        "event_type    as soc_type",
        "country       as soc_country",
        "ts            as soc_ts",
        "source        as soc_source"
      )

    println(s"[04] official rows: ${official.count()},  social rows: ${social.count()}")

    // Spark SQL: Performance subtag — broadcast the smaller side if it fits.
    val socialBroadcast = if (social.count() < 200000) broadcast(social) else social

    val joined = official.join(
      socialBroadcast,
      col("off_type") === col("soc_type") &&
      col("off_country") === col("soc_country") &&
      abs(unix_timestamp(col("soc_ts")) - unix_timestamp(col("off_ts"))) <= WINDOW_SECONDS
    )

    val matches = joined.withColumn(
      "delta_seconds",
      unix_timestamp(col("soc_ts")) - unix_timestamp(col("off_ts"))
    ).select(
      col("off_type").alias("event_type"),
      col("off_country").alias("country"),
      col("off_id"), col("soc_id"),
      col("off_source"), col("soc_source"),
      col("off_ts"), col("soc_ts"),
      col("delta_seconds"),
      col("off_sev").alias("severity")
    )

    val matchCount = matches.count()
    println(s"[04] matched pairs: $matchCount")

    matches.write.mode("overwrite").parquet(MATCHES_PATH)

    if (matchCount > 0) {
      // Aggregate via Spark SQL.
      matches.createOrReplaceTempView("lt_matches")
      val summary = spark.sql(
        """
          |SELECT
          |  event_type,
          |  COUNT(*)                                                   AS n_pairs,
          |  ROUND(AVG(delta_seconds) / 60.0, 1)                        AS mean_delta_minutes,
          |  ROUND(percentile_approx(delta_seconds, 0.5) / 60.0, 1)     AS median_delta_minutes,
          |  ROUND(percentile_approx(delta_seconds, 0.1) / 60.0, 1)     AS p10_delta_minutes,
          |  ROUND(percentile_approx(delta_seconds, 0.9) / 60.0, 1)     AS p90_delta_minutes,
          |  SUM(CASE WHEN delta_seconds < 0 THEN 1 ELSE 0 END)         AS n_social_first,
          |  SUM(CASE WHEN delta_seconds > 0 THEN 1 ELSE 0 END)         AS n_official_first
          |FROM lt_matches
          |GROUP BY event_type
          |ORDER BY n_pairs DESC
        """.stripMargin)

      summary.show(false)
      summary.write.mode("overwrite").parquet(SUMMARY_PATH)
    } else {
      println("[04] no matches; writing empty summary")
      Seq.empty[(String, Long, Double, Double, Double, Double, Long, Long)]
        .toDF(
          "event_type",
          "n_pairs",
          "mean_delta_minutes",
          "median_delta_minutes",
          "p10_delta_minutes",
          "p90_delta_minutes",
          "n_social_first",
          "n_official_first"
        )
        .write.mode("overwrite").parquet(SUMMARY_PATH)
    }

    println("[04] done")
    spark.stop()
  }
}
