#!/usr/bin/env bash
# Push everything under ./data/raw/ into HDFS at /raw/<source>/.
# Idempotent: skips a source if its HDFS dir already non-empty.
set -euo pipefail

cd "$(dirname "$0")/.."

export HADOOP_HOME=${HADOOP_HOME:-/home/jrf/hadoop}
export PATH=$HADOOP_HOME/bin:$PATH

HDFS="hdfs dfs"

echo "[load_to_hdfs] checking HDFS NameNode"
hdfs dfsadmin -safemode get >/dev/null

echo "[load_to_hdfs] creating HDFS base dirs"
$HDFS -mkdir -p /raw /processed /mr_out /mr_in /incoming 2>/dev/null || true

push() {
    local src="$1"
    local dst="$2"
    local local_dir="data/raw/${src}"

    if ! [ -d "$local_dir" ] || [ -z "$(ls -A "$local_dir" 2>/dev/null)" ]; then
        echo "[load_to_hdfs] SKIP ${src}: no local files"
        return
    fi

    local count
    count=$($HDFS -ls "/raw/${dst}" 2>/dev/null | grep -c "^-" || true)
    if [ "$count" -gt 0 ]; then
        echo "[load_to_hdfs] SKIP ${src}: /raw/${dst} already has ${count} files"
        return
    fi

    echo "[load_to_hdfs] put ${src} -> /raw/${dst}"
    $HDFS -mkdir -p "/raw/${dst}"
    $HDFS -put -f "${local_dir}/." "/raw/${dst}/"
    echo "[load_to_hdfs]   done: $(du -sh ${local_dir} | cut -f1)"
}

push usgs      usgs
push eonet     eonet
push reliefweb reliefweb
push gdacs     gdacs
push gdelt     gdelt
push social    social

echo
echo "[load_to_hdfs] HDFS /raw layout:"
$HDFS -du -h /raw
