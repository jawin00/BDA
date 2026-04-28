# Disaster Intelligence Lakehouse

A college Big Data Analytics coursework project. Multi-source historical disaster intelligence pipeline: ingest seven public sources into HDFS, enrich with Spark + MapReduce + Scala (zero-shot classification, severity scoring, geoparsing, K-means clustering, lead-time analysis), serve through MongoDB + Cassandra + Spark SQL Thrift Server, and visualize in a Streamlit dashboard.

The full plan, syllabus mapping, and architecture rationale live in [`docs/`](docs/).

## Syllabus topics covered

NoSQL, MongoDB, Apache Cassandra, Apache Hadoop, HDFS, MapReduce, Scala, Apache Spark, Spark SQL (linking, using in applications, loading and saving data, JDBC/ODBC server, user-defined functions, performance).

See [`docs/syllabus_mapping.md`](docs/syllabus_mapping.md) for a tag-by-tag map to the exact files that exercise each topic.

## Prerequisites

- Linux host, ~16 GB RAM, ≥ 30 GB free disk.
- Docker Engine + Docker Compose v2.
- (Optional, for the GPU-accelerated NLP UDFs) NVIDIA GPU + recent driver + NVIDIA Container Toolkit:
  ```bash
  sudo apt install -y nvidia-container-toolkit
  sudo systemctl restart docker
  docker run --rm --gpus all nvidia/cuda:12.4.0-base-ubuntu22.04 nvidia-smi
  ```
- A Reddit script-app (free, 5 minutes): https://www.reddit.com/prefs/apps
- A self-chosen ReliefWeb `appname` string (any short identifier).

## Setup

```bash
cp .env.example .env
# edit .env: fill REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT, RELIEFWEB_APPNAME
docker compose build           # builds the GPU-enabled Spark image (~10 min first time)
docker compose up -d           # boots HDFS, Spark, MongoDB, Cassandra, Thrift Server
./download_all.sh              # fetches ~7 GB historical disaster data into ./data/raw/
                               # use DOWNLOAD_PROFILE=small in .env for a 10× smaller subset
./ingest/load_to_hdfs.sh       # copies raw data into HDFS
./run_pipeline.sh              # runs all 6 pipeline steps
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py # opens http://localhost:8501
```

## Verifying

```bash
# HDFS web UI
xdg-open http://localhost:9870

# MongoDB count
docker exec mongodb mongosh --quiet --eval 'db.getSiblingDB("disasters").events.countDocuments()'

# Cassandra count
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM disasters.events_by_region_date;"

# Spark SQL via JDBC (proves Thrift Server is up — Spark SQL JDBC/ODBC subtag)
docker exec -it spark-thrift /opt/spark/bin/beeline -u jdbc:hive2://spark-thrift:10000 \
  -e "SELECT event_type, COUNT(*) AS n FROM events_enriched GROUP BY event_type ORDER BY n DESC;"
```

## Project layout

```
disaster-intel/
├── docker-compose.yml          # HDFS + Spark (GPU) + Thrift + MongoDB + Cassandra
├── docker/spark/               # custom CUDA + Spark + PyTorch image
├── download_all.sh             # one-time fetch of ~7 GB historical data
├── run_pipeline.sh             # runs the 6 pipeline steps end to end
├── run_streaming.sh            # optional: starts the Spark Structured Streaming demo
├── ingest/                     # HDFS load scripts + per-source downloaders
├── pipeline/
│   ├── 01_clean_normalize.py        # PySpark + Spark SQL
│   ├── 02_enrich.py                 # PySpark with GPU UDFs
│   ├── 03_keyword_freq_mr/          # Java Hadoop MapReduce
│   ├── 04_lead_time_analyzer/       # Scala Spark
│   ├── 05_cluster_themes.py         # PySpark MLlib (TF-IDF + K-means)
│   ├── 06_load_serving_stores.py    # MongoDB + Cassandra writers
│   └── streaming/file_source_stream.py
├── udfs/                       # GPU classify, severity, geoparse
├── thrift/                     # Spark Thrift Server entrypoint
├── dashboard/                  # Streamlit (8 batch pages + Bluesky live + Reddit live)
└── docs/                       # architecture, viva_questions, syllabus_mapping, impact_writeup
```

## Running the streaming extension (optional, for the viva)

```bash
./run_streaming.sh             # starts Spark Structured Streaming on /work/data/incoming/
# in another terminal, drop a JSON event:
cp data/samples/sample_event.json data/incoming/
# refresh the dashboard — new event appears within ~30 s
```

## Notes

- The pipeline is batch-first by design. A streaming extension is included (Spark Structured Streaming file source + a live Reddit poller dashboard tab) to demonstrate the streaming engine without dragging Kafka into the stack.
- The two transformer-based UDFs (DeBERTa-v3 zero-shot classifier, MiniLM sentence embedder) run on the GPU. They will **fail loudly** if CUDA is unavailable rather than silently fall back to CPU. All other components are CPU-only and unaffected.
