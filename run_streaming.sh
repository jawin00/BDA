#!/usr/bin/env bash
# Optional: start the Spark Structured Streaming file-source job.
# Drops files into ./data/incoming/ (mounted into the spark container at /work/data/incoming).
set -euo pipefail

cd "$(dirname "$0")"

mkdir -p data/incoming data/streaming_checkpoint

echo "[streaming] starting Spark Structured Streaming on /work/data/incoming/"
echo "[streaming] In another terminal:  cp data/samples/sample_event.json data/incoming/"
echo "[streaming] Ctrl+C to stop."

docker exec -it spark /opt/spark/bin/spark-submit \
    --master "local[*]" \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
    /work/pipeline/streaming/file_source_stream.py
