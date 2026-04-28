#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

export HADOOP_HOME=${HADOOP_HOME:-/home/jrf/hadoop}
export SPARK_HOME=${SPARK_HOME:-/home/jrf/spark}
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export PATH=$SPARK_HOME/bin:$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH

VENV=${VENV:-$(pwd)/.venv}
PYTHON=$VENV/bin/python

INPUT="hdfs://localhost:9000/mr_in/keyword_freq"
OUTPUT="hdfs://localhost:9000/mr_out/keyword_freq"

echo "[03] staging TSV input via Spark"
PYSPARK_PYTHON=$PYTHON \
spark-submit \
    --master "local[*]" \
    --conf "spark.hadoop.fs.defaultFS=hdfs://localhost:9000" \
    --conf "spark.driver.memory=2g" \
    pipeline/03_keyword_freq_mr/stage_input.py

echo "[03] building MR jar with Maven"
mvn -q -DskipTests package -f pipeline/03_keyword_freq_mr/pom.xml

JAR=pipeline/03_keyword_freq_mr/target/keyword-freq-mr.jar
[ -f "$JAR" ] || { echo "[03] jar not found: $JAR"; exit 1; }

echo "[03] clearing previous MR output"
hdfs dfs -rm -r -f -skipTrash "$OUTPUT" 2>/dev/null || true

echo "[03] submitting MapReduce job"
HADOOP_CLASSPATH=$JAR hadoop jar "$JAR" edu.bda.mr.KeywordFreqMR "$INPUT" "$OUTPUT"

echo "[03] sample output:"
hdfs dfs -cat "${OUTPUT}/part-r-00000" 2>/dev/null | head -20
