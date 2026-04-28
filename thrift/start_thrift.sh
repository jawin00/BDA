#!/usr/bin/env bash
# Start the Spark SQL Thrift Server with our enriched Parquet tables
# pre-registered as views, so beeline / dashboards can query them over JDBC.
set -euo pipefail

# Pre-register tables by writing a SQL bootstrap and pointing the Thrift
# server at it via spark.sql.warehouse.dir + an init script.
INIT_SQL=/tmp/thrift_init.sql
cat > "${INIT_SQL}" <<'SQL'
DROP TABLE IF EXISTS events_enriched;
CREATE TABLE events_enriched USING parquet
  LOCATION 'hdfs://localhost:9000/processed/events_enriched';

DROP TABLE IF EXISTS events_clustered;
CREATE TABLE events_clustered USING parquet
  LOCATION 'hdfs://localhost:9000/processed/events_clustered';

DROP TABLE IF EXISTS lead_time_matches;
CREATE TABLE lead_time_matches USING parquet
  LOCATION 'hdfs://localhost:9000/processed/lead_time/matches';

DROP TABLE IF EXISTS lead_time_summary;
CREATE TABLE lead_time_summary USING parquet
  LOCATION 'hdfs://localhost:9000/processed/lead_time/summary';

DROP TABLE IF EXISTS cluster_terms;
CREATE TABLE cluster_terms USING parquet
  LOCATION 'hdfs://localhost:9000/processed/cluster_terms';
SQL

mkdir -p /opt/spark/logs

# Start the Thrift server. It binds 0.0.0.0:10000 by default. We use
# --hiveconf to inject the init script via spark-sql --init-file equivalent
# (Thrift Server doesn't support --init-file, so we use a startup hook
# pattern: a wrapper that runs the init SQL after the service is up).
nohup /opt/spark/sbin/start-thriftserver.sh \
    --master "local[*]" \
    --conf spark.sql.warehouse.dir=/tmp/spark-warehouse \
    --conf spark.sql.adaptive.enabled=true \
    --hiveconf hive.server2.thrift.port=10000 \
    --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
    > /opt/spark/logs/thrift.out 2>&1

# Wait for the server to be ready, then push the view registrations.
echo "[thrift] waiting for server"
for i in $(seq 1 60); do
    if /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

echo "[thrift] registering views"
/opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 -f "${INIT_SQL}" || \
    echo "[thrift] view init failed (non-fatal — pipeline may not have produced outputs yet)"

echo "[thrift] ready on jdbc:hive2://localhost:10000"
