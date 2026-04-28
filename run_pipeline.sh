#!/usr/bin/env bash
# Run all 6 pipeline steps natively (no Docker).
set -euo pipefail

cd "$(dirname "$0")"

export HADOOP_HOME=${HADOOP_HOME:-/home/jrf/hadoop}
export SPARK_HOME=${SPARK_HOME:-/home/jrf/spark}
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$SPARK_HOME/bin:$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH

# Load .env if present
[ -f .env ] && export $(grep -v '^#' .env | xargs)

SKIP_GPU=${SKIP_GPU:-0}
VENV=${VENV:-$(pwd)/.venv}
PYTHON=$VENV/bin/python

echo "==[run_pipeline] SKIP_GPU=$SKIP_GPU"

# Packages needed for step 6 (Mongo + Cassandra connectors)
CONNECTORS="org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"

run_spark() {
    local script="$1"
    shift
    PYSPARK_PYTHON=$PYTHON SKIP_GPU=$SKIP_GPU \
    spark-submit \
        --master "local[*]" \
        --conf "spark.hadoop.fs.defaultFS=hdfs://localhost:9000" \
        --conf "spark.driver.memory=3g" \
        --conf "spark.executor.memory=3g" \
        --conf "spark.sql.adaptive.enabled=true" \
        "$@" \
        "$script"
}

# 0. Load data into HDFS
echo
echo "==[run_pipeline] step 0: HDFS load"
bash ingest/load_to_hdfs.sh

# 1. clean + normalize
echo
echo "==[run_pipeline] step 1: clean + normalize"
run_spark pipeline/01_clean_normalize.py

# Validate
COUNT=$(hdfs dfs -ls /processed/events_raw/ 2>/dev/null | grep -c "^-" || true)
[ "$COUNT" -gt 0 ] || { echo "step 1 produced no output"; exit 1; }

# 2. enrich
echo
echo "==[run_pipeline] step 2: enrich (SKIP_GPU=$SKIP_GPU)"
run_spark pipeline/02_enrich.py

# 3. MapReduce keyword frequency
echo
echo "==[run_pipeline] step 3: MapReduce"
bash pipeline/03_keyword_freq_mr/build_and_run.sh

# 4. Scala lead-time analyzer
echo
echo "==[run_pipeline] step 4: Scala job"
bash pipeline/04_lead_time_analyzer/build_and_run.sh

# 5. cluster themes
echo
echo "==[run_pipeline] step 5: cluster themes"
run_spark pipeline/05_cluster_themes.py

# 6. load to MongoDB + Cassandra
echo
echo "==[run_pipeline] step 6: load serving stores"
run_spark --packages "$CONNECTORS" pipeline/06_load_serving_stores.py

echo
echo "==[run_pipeline] all done"
echo "Start Thrift Server: bash thrift/start_thrift.sh"
echo "Start dashboard:     $VENV/bin/streamlit run dashboard/app.py"
