# Disaster Intelligence Lakehouse — CLAUDE.md

## Project overview

Big Data Analytics college coursework. Multi-source historical disaster intelligence pipeline:
ingest 7 public sources → HDFS → Spark + MapReduce + Scala enrichment → MongoDB + Cassandra →
Streamlit dashboard.

**Platform: Windows 11.** Use PowerShell scripts (`.ps1`), not the `.sh` files.

## Stack

| Layer | Technology |
|-------|-----------|
| Storage | HDFS (Hadoop local mode) |
| Batch processing | PySpark, Java Hadoop MapReduce, Scala Spark |
| Serving stores | MongoDB, Apache Cassandra, Spark SQL Thrift Server |
| Dashboard | Streamlit (Python) |
| Build tools | Maven (Java MR jar), sbt (Scala fat jar) |

## Windows run commands

All scripts live at the project root or alongside their Linux counterparts. Run from the
project root in PowerShell:

```powershell
# 0. Set required env vars (edit to match your install paths)
$env:HADOOP_HOME = "C:\hadoop"
$env:SPARK_HOME  = "C:\spark"
$env:JAVA_HOME   = "C:\Program Files\Eclipse Adoptium\jdk-11.0.x-hotspot"

# 1. Download raw data
.\download_all.ps1

# 2. Load into HDFS
.\ingest\load_to_hdfs.ps1

# 3. Run the full pipeline (steps 1-6)
.\run_pipeline.ps1

# 4. Start Thrift Server
.\thrift\start_thrift.ps1

# 5. Start dashboard
.\.venv\Scripts\streamlit run dashboard\app.py

# Optional: streaming demo
.\run_streaming.ps1
```

## Environment variables

Copy `.env.example` to `.env` and fill in:
- `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, `REDDIT_USER_AGENT`
- `RELIEFWEB_APPNAME`
- `DOWNLOAD_PROFILE=small` (optional, fetches ~700 MB instead of ~7 GB)
- `SKIP_GPU=1` (set to 1 if no NVIDIA GPU)
- `HADOOP_HOME`, `SPARK_HOME`, `JAVA_HOME` (can override here instead of shell)

## Project layout

```
BDA/
├── CLAUDE.md
├── download_all.ps1          # fetch raw data (Windows)
├── run_pipeline.ps1          # run all 6 pipeline steps (Windows)
├── run_streaming.ps1         # optional streaming demo (Windows)
├── ingest/
│   ├── load_to_hdfs.ps1      # push raw data into HDFS (Windows)
│   └── download_*.py
├── pipeline/
│   ├── 01_clean_normalize.py
│   ├── 02_enrich.py
│   ├── 03_keyword_freq_mr/
│   │   └── build_and_run.ps1
│   ├── 04_lead_time_analyzer/
│   │   └── build_and_run.ps1
│   ├── 05_cluster_themes.py
│   ├── 06_load_serving_stores.py
│   └── streaming/file_source_stream.py
├── udfs/                     # classify.py, geoparse.py, severity.py
├── thrift/
│   └── start_thrift.ps1      # Spark SQL Thrift Server (Windows)
└── dashboard/
    ├── app.py
    └── requirements.txt
```

## Syllabus topics

NoSQL, MongoDB, Apache Cassandra, Apache Hadoop, HDFS, MapReduce, Scala, Apache Spark,
Spark SQL (linking, loading/saving, JDBC/ODBC Thrift Server, UDFs, performance).

## Windows prerequisites

- Java 11 (e.g. Eclipse Temurin / Adoptium)
- Apache Hadoop 3.x (winutils required — see https://github.com/cdarlint/winutils)
- Apache Spark 3.x
- Python 3.10+
- Maven (for step 3 MapReduce jar)
- sbt (for step 4 Scala job)
- MongoDB and Cassandra running locally (or via Docker Desktop on Windows)
