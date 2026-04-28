#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

export HADOOP_HOME=${HADOOP_HOME:-/home/jrf/hadoop}
export SPARK_HOME=${SPARK_HOME:-/home/jrf/spark}
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export PATH=$SPARK_HOME/bin:$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH

JOB_DIR=pipeline/04_lead_time_analyzer

echo "[04] building Scala fat jar with sbt"
cd "$JOB_DIR"
sbt -batch assembly
cd "$(dirname "$0")/../.."

JAR=${JOB_DIR}/target/scala-2.12/lead-time-analyzer-assembly.jar
[ -f "$JAR" ] || { echo "[04] jar not found: $JAR"; exit 1; }

echo "[04] submitting Scala Spark job"
spark-submit \
    --master "local[*]" \
    --conf "spark.hadoop.fs.defaultFS=hdfs://localhost:9000" \
    --conf "spark.driver.memory=2g" \
    --class edu.bda.LeadTimeAnalyzer \
    "$JAR"
