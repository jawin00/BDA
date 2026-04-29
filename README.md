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

## Setup (Linux / Docker)

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

## Setup (Windows Native / PowerShell)

If running natively on Windows without Docker, you must have Hadoop, Spark, MongoDB, and Cassandra installed locally.

```powershell
# 1. Set environment variables
$env:JAVA_HOME = "C:\jdk11"
$env:HADOOP_HOME = "C:\hadoop"
$env:SPARK_HOME = "C:\spark"

# 2. Start HDFS
cmd /c "C:\hadoop\sbin\start-dfs.cmd"

# 3. Start Cassandra (requires JRE 8 specifically)
$env:JAVA_HOME = "C:\path\to\jre8"
Start-Process -FilePath "C:\cassandra\bin\cassandra.bat" -ArgumentList "-f" -WindowStyle Hidden

# 4. Run the full data pipeline
.\run_pipeline.ps1

# 5. Start the Spark Thrift Server (for SQL queries)
.\thrift\start_thrift.ps1

# 6. Run the Streamlit Dashboard
.\.venv\Scripts\streamlit run dashboard\app.py
```

## Analytics included

The project now includes a full analytics layer on top of the storage and processing pipeline. The dashboard is not just a UI shell around the raw tables - it is an end-to-end analytics surface that validates the HDFS outputs, Spark SQL models, Cassandra time-series table, MongoDB serving collection, and the live Bluesky ingestion path.

### Core analytics themes

- Lakehouse KPI monitoring: total events, active sources, countries covered, event-type diversity, average and median severity, average daily volume, peak-day volume, and country concentration.
- Temporal analytics: daily source trends, 7-day rolling averages, source share over time, top event-type trend lines, weekday-by-hour activity heatmaps, and source burstiness analysis.
- Risk and severity analytics: severity histograms, event-type severity box plots, severity-band mix by source, average-severity heatmaps, and country-level risk scatter plots.
- Geographic analytics: top countries by volume, monthly trend lines for the most affected regions, source-by-country contribution matrices, and latest-activity country summaries.
- Data quality analytics: raw coordinate coverage, geoparse coverage, country-population coverage, text completeness, classification-confidence coverage, daily quality trends, and per-source completeness heatmaps.
- Theme and cluster analytics: cluster sizes, average severity per cluster, dominant event type per cluster, top cluster terms, and cluster-by-event-type intensity matrices.
- Operational analytics: Spark SQL performance benchmarking, Cassandra time-series rollups, MongoDB exploration, and live authenticated Bluesky ingestion metrics.

### Example analytical questions the project can answer

- Which disaster types are accelerating week over week, and which sources are driving the increase?
- Which countries have both high event volume and high average severity in the current window?
- How complete is the enriched data by source, and where does geoparsing add the most value?
- Which clusters contain the most severe events, and what terms dominate those themes?
- How much faster do Spark SQL queries become with caching and broadcast hints?
- How many recent live social posts are being captured, and are they reaching the serving store correctly?

### Dashboard walkthrough

1. World Map: interactive global event map backed by MongoDB, filtered by event type, severity, and marker volume.
2. MongoDB Explorer: document-level browsing and filtering against the serving collection used for fast dashboard reads.
3. Cassandra Time-Series: region and month oriented analytics designed around the Cassandra partition key.
4. Cluster Themes: TF-IDF plus K-means results with top titles and dominant terms by cluster.
5. MapReduce Results: Hadoop MapReduce keyword frequencies over the enriched event corpus.
6. Spark SQL Playground: free-form SQL execution through the Thrift Server for ad hoc analytics.
7. Performance Lab: side-by-side timing experiments for cold runs, cached runs, and broadcast-hint runs.
8. Live Bluesky Disaster Stream: live social-signal collection into MongoDB with public or authenticated Bluesky search.
9. Analytics Hub: cross-source KPI, temporal, severity, geography, quality, and cluster analytics in one place.

These analytics are powered by the same lakehouse outputs exposed through Spark Thrift, MongoDB, and Cassandra, so they double as both insight views and proof that the processing pipeline is actually producing usable analytical tables.

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
├── dashboard/                  # Streamlit dashboard with maps, live feeds, and analytics hub
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
