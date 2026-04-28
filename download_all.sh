#!/usr/bin/env bash
# Fetch all 6 historical disaster sources into ./data/raw/.
# Honors DOWNLOAD_PROFILE=small in .env for a 10x smaller subset.
set -euo pipefail

cd "$(dirname "$0")"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . .env
  set +a
fi

PROFILE="${DOWNLOAD_PROFILE:-full}"
SMALL_FLAG=""
if [ "$PROFILE" = "small" ]; then
  SMALL_FLAG="--small"
  echo "[download_all] DOWNLOAD_PROFILE=small — fetching reduced subset"
else
  echo "[download_all] DOWNLOAD_PROFILE=full — fetching ~7 GB"
fi

PY="${PYTHON:-python3}"

# Local-host venv for downloaders (no Spark image needed for this step).
if [ ! -d .venv-ingest ]; then
  echo "[download_all] creating .venv-ingest for downloader deps"
  $PY -m venv .venv-ingest
  ./.venv-ingest/bin/pip install --quiet --upgrade pip
  ./.venv-ingest/bin/pip install --quiet requests tqdm
fi
PY=./.venv-ingest/bin/python

echo "[download_all] 1/6 USGS earthquakes"
$PY ingest/download_usgs.py $SMALL_FLAG

echo "[download_all] 2/6 NASA EONET"
$PY ingest/download_eonet.py $SMALL_FLAG

echo "[download_all] 3/6 ReliefWeb reports"
$PY ingest/download_reliefweb.py $SMALL_FLAG

echo "[download_all] 4/6 GDACS alerts"
$PY ingest/download_gdacs.py $SMALL_FLAG

echo "[download_all] 5/6 GDELT events (filtered)"
$PY ingest/download_gdelt.py $SMALL_FLAG

echo "[download_all] 6/6 HumAID labeled tweets"
$PY ingest/download_humaid.py $SMALL_FLAG

echo "[download_all] bonus: GeoNames cities500 lexicon"
$PY ingest/download_geonames.py

echo
echo "[download_all] done. Sizes:"
du -sh data/raw/* 2>/dev/null || true
echo
echo "Next: ./ingest/load_to_hdfs.sh"
