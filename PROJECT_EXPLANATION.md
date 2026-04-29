# Disaster Intelligence Lakehouse - Deep Project Explanation

This file is a full walkthrough of what the project does, why each part exists,
how the pieces connect, and what is special about the current Windows-native
version of the stack.

## 1. What this project is

This repository is a disaster-intelligence lakehouse built for a Big Data
Analytics course. The goal is not just to collect disaster data, but to show a
complete end-to-end data system:

- ingest multiple public data sources
- land them in distributed storage
- normalize them into one event model
- enrich them with analytics-ready attributes
- run multiple compute paradigms over the same data
- load serving stores optimized for different read patterns
- expose the results through a dashboard and SQL interface

In other words, this project is both:

- a working analytics application
- a demonstration of syllabus topics such as HDFS, Spark SQL, MapReduce,
  Scala Spark, MongoDB, Cassandra, JDBC/ODBC access, UDFs, clustering, and
  performance tuning

## 2. Why it is called a lakehouse

The project behaves like a small lakehouse because it combines:

- a data lake style landing zone in HDFS for raw and processed files
- warehouse-style structured tables exposed through Spark SQL Thrift
- serving-layer databases for application-style reads

The same dataset moves through several levels of refinement:

1. raw source files on local disk
2. raw source folders in HDFS
3. normalized Parquet in HDFS
4. enriched Parquet in HDFS
5. specialized analytical outputs such as keyword frequency, lead-time, and clusters
6. serving copies in MongoDB and Cassandra
7. queryable views in Spark Thrift and visualizations in Streamlit

That layered architecture is the main reason this is more than a simple ETL
script.

## 3. Big-picture architecture

```text
Public sources
  |
  v
download_* scripts
  |
  v
data/raw/<source> on local disk
  |
  v
ingest/load_to_hdfs.ps1
  |
  v
HDFS /raw/<source>
  |
  v
01_clean_normalize.py
  |
  v
HDFS /processed/events_raw
  |
  v
02_enrich.py + UDFs
  |
  v
HDFS /processed/events_enriched
  |            |                 |
  |            |                 |
  |            |                 +--> 05_cluster_themes.py
  |            |                       -> /processed/events_clustered
  |            |                       -> /processed/cluster_terms
  |            |
  |            +--> 03_keyword_freq_mr
  |                 -> /mr_out/keyword_freq
  |
  +--> 04_lead_time_analyzer
       -> /processed/lead_time/*

All processed outputs
  |
  v
06_load_serving_stores.py
  |
  +--> MongoDB.disasters.events
  +--> MongoDB.disasters.keyword_freq
  +--> Cassandra.disasters.events_by_region_date

HDFS processed outputs
  |
  v
thrift/start_thrift.ps1
  |
  v
Spark Thrift Server external tables
  |
  v
Streamlit dashboard + SQL playground + performance lab
```

## 4. Main problem the project solves

Disaster data is fragmented.

Different sources report:

- different event types
- different timestamp formats
- different location quality
- different amounts of text
- different update frequencies

This project solves that by converting heterogeneous disaster records into one
shared event pipeline. Once everything is in a shared structure, the data can be
queried uniformly:

- by geography
- by time
- by severity
- by source
- by cluster/theme
- by event type

The project is therefore really about schema unification plus layered analytics.

## 5. Data sources

The batch pipeline uses six core historical sources, plus one live dashboard
source and one supporting lexicon.

### 5.1 Core historical sources

1. USGS
   Earthquake events with structured coordinates and timestamps.

2. NASA EONET
   Natural event feeds such as wildfires, storms, floods, and volcanoes.

3. GDACS
   Official disaster alerts, especially useful as a structured source of event
   categories.

4. GDELT
   Very large global events/news feed. This is usually the dominant source by
   volume and runtime cost.

5. ReliefWeb
   Humanitarian and disaster reporting text.

6. HumAID / social
   Social-media style disaster text used for social-signal analysis.

### 5.2 Supporting source

7. GeoNames
   Used as a reference lexicon for geoparsing rather than as an event stream.

### 5.3 Live source

8. Bluesky search
   Used by the live dashboard page to ingest recent public posts into MongoDB.
   This is operationally separate from the main historical batch pipeline.

## 6. Repository structure

The most important folders are:

- [ingest/](ingest/)  
  Per-source downloaders and HDFS load scripts.

- [pipeline/](pipeline/)  
  The six batch pipeline steps plus shared HDFS commit helpers and streaming code.

- [udfs/](udfs/)  
  Classification, severity, and geoparsing logic.

- [thrift/](thrift/)  
  Spark Thrift startup and table registration.

- [dashboard/](dashboard/)  
  Streamlit app, pages, and database clients.

- [.hdfs/](.hdfs/)  
  Project-local HDFS data directories in the current Windows-native setup.

## 7. The canonical event model

The project becomes coherent at step 1, when all sources are projected into a
single shared schema.

### 7.1 Normalized schema

Step 1 writes rows with these core columns:

- `event_id`
- `source`
- `title`
- `text`
- `lat`
- `lon`
- `ts`
- `country`
- `raw_json`

Why each matters:

- `event_id`: per-source identity for traceability and deduplication
- `source`: keeps source provenance after schema unification
- `title` and `text`: the main analytical text fields
- `lat`, `lon`, `country`: geographic filtering and mapping
- `ts`: the common event time axis
- `raw_json`: lets you trace any derived row back to the raw record

### 7.2 Enriched schema

Step 2 adds:

- `event_type`
- `classify_conf`
- `severity`
- `geo_lat`
- `geo_lon`
- `geo_country`

These turn raw events into something analytical:

- `event_type` supports category analytics
- `severity` supports ranking and risk summaries
- geoparse fallback fields support better location coverage
- `classify_conf` is useful for confidence and quality analysis

### 7.3 Clustered schema

Step 5 adds:

- `cluster_id`

This creates a thematic grouping layer on top of the enriched events.

## 8. Step-by-step pipeline explanation

## 8.1 Download layer

Relevant files:

- [download_all.ps1](download_all.ps1)
- [ingest/download_usgs.py](ingest/download_usgs.py)
- [ingest/download_eonet.py](ingest/download_eonet.py)
- [ingest/download_reliefweb.py](ingest/download_reliefweb.py)
- [ingest/download_gdacs.py](ingest/download_gdacs.py)
- [ingest/download_gdelt.py](ingest/download_gdelt.py)
- [ingest/download_humaid.py](ingest/download_humaid.py)
- [ingest/download_geonames.py](ingest/download_geonames.py)

What happens:

- each downloader fetches one source
- each source lands under `data/raw/<source>/`
- the downloader layer keeps source-specific logic isolated from the rest of the system

Why this separation is important:

- source-specific APIs and formats remain local to the downloader
- downstream pipeline steps can work against files, not external APIs
- reruns become reproducible because the raw landing files are preserved

## 8.2 HDFS load

Relevant files:

- [ingest/load_to_hdfs.ps1](ingest/load_to_hdfs.ps1)
- [ingest/load_to_hdfs.sh](ingest/load_to_hdfs.sh)

What happens:

- HDFS directories such as `/raw`, `/processed`, `/mr_in`, `/mr_out`, and
  `/incoming` are prepared
- local raw files are copied into HDFS
- the batch pipeline from this point onward reads from HDFS, not directly from
  local downloader outputs

Why this matters:

- it exercises Hadoop storage as a first-class layer
- it keeps the rest of the pipeline consistent with distributed storage
- Spark, MapReduce, and Thrift can all point at the same HDFS locations

## 8.3 Step 1 - clean and normalize

File:

- [pipeline/01_clean_normalize.py](pipeline/01_clean_normalize.py)

Purpose:

- convert all sources into one common event model
- filter unusable rows
- preserve provenance

What it does:

- defines one loader function per source
- reads HDFS raw folders such as `/raw/usgs`, `/raw/eonet`, `/raw/gdelt`, etc.
- flattens source-specific fields into the canonical schema
- keeps `raw_json` for traceability
- writes one output folder under `/processed/events_raw`

Important design choices:

- the loaders are source-aware, not generic, because the formats are too different
- rows are filtered after normalization so that downstream steps see consistent
  columns
- output is stored in per-source subdirectories rather than Hive-style
  `source=value` partitions in the Windows-native build

One important recent fix:

- GDELT latitude, longitude, country, and text column offsets were corrected in
  the loader so that map coordinates and geography analytics are valid

## 8.4 Step 2 - enrich

File:

- [pipeline/02_enrich.py](pipeline/02_enrich.py)

Purpose:

- make the normalized events analytics-ready

Enrichment logic:

1. Event typing
   - USGS, EONET, and GDACS use rule-based mapping because they already carry
     structured or semi-structured category hints
   - social, ReliefWeb, and GDELT can use the classifier for free-form text

2. Severity scoring
   - a UDF scores text/title into a numeric severity field

3. Geoparsing
   - if raw country or coordinates are missing, the geoparse UDF tries to infer
     location from text

4. Final location fusion
   - raw and inferred geographic signals are combined to improve coverage

Performance-aware details:

- expensive classification is limited to the sources that actually need it
- geoparsing is skipped for rows that already have solid country and coordinate
  fields

This step is where raw events become analytical entities.

## 8.5 Step 3 - MapReduce keyword frequency

Files:

- [pipeline/03_keyword_freq_mr/stage_input.py](pipeline/03_keyword_freq_mr/stage_input.py)
- [pipeline/03_keyword_freq_mr/build_and_run.ps1](pipeline/03_keyword_freq_mr/build_and_run.ps1)

Purpose:

- demonstrate classic Hadoop MapReduce on the same dataset
- produce a simple corpus-wide keyword aggregation

What happens:

- Parquet from `events_enriched` is staged into a flat TSV-friendly form
- a Java MapReduce job counts keywords by year and country
- results are written to HDFS under `/mr_out/keyword_freq`

Why this stage exists:

- it proves the project is not Spark-only
- it shows how the same enriched lakehouse data can feed a different compute
  model

## 8.6 Step 4 - Scala lead-time analyzer

Files:

- [pipeline/04_lead_time_analyzer/build_and_run.ps1](pipeline/04_lead_time_analyzer/build_and_run.ps1)

Purpose:

- demonstrate Scala Spark
- measure lag between social-style signals and official reporting

What it conceptually computes:

- match or compare social-signal rows against official-source rows
- estimate a lead or lag in minutes
- produce both detailed matches and a summary table

Outputs:

- `/processed/lead_time/matches`
- `/processed/lead_time/summary`

Important caveat:

- when `SKIP_GPU=1` is enabled, classification becomes more limited
- that can collapse many text-heavy rows into `other`
- as a result, the lead-time job may produce zero matched pairs even though the
  job itself ran correctly

So a zero-row lead-time summary is not automatically a pipeline failure.

## 8.7 Step 5 - cluster themes

File:

- [pipeline/05_cluster_themes.py](pipeline/05_cluster_themes.py)

Purpose:

- demonstrate MLlib
- discover themes/topics across the event corpus

Method:

- concatenate `title` and `text`
- tokenize
- remove stop words
- compute hashed TF
- compute IDF
- fit K-means with `k=12`

Outputs:

- `/processed/events_clustered`
- `/processed/cluster_terms`

Scaling choice in the current repo:

- fitting is done on a sample when the full dataset is too large for local-mode
  K-means
- predictions are then generated across the full dataset

That is a practical tradeoff for a Windows-native local environment handling
millions of rows.

## 8.8 Step 6 - load serving stores

File:

- [pipeline/06_load_serving_stores.py](pipeline/06_load_serving_stores.py)

Purpose:

- move from analytical files to application-friendly query stores

This step loads:

- `MongoDB.disasters.events`
- `MongoDB.disasters.keyword_freq`
- `Cassandra.disasters.events_by_region_date`

Why two serving databases are used:

- MongoDB is good for flexible document reads, filtering, and pagination
- Cassandra is good for fast time-series scans when the access pattern is known

This separation is an important architectural choice rather than duplication.

## 9. Why MongoDB and Cassandra are both present

The project intentionally uses two NoSQL systems because they serve different
query patterns.

### 9.1 MongoDB role

MongoDB is used for:

- event browsing
- document-style filtering by source, event type, severity, and geography
- powering the world map and explorer pages
- storing keyword-frequency rows for dashboard access
- receiving live Bluesky events

Why MongoDB fits:

- flexible schema
- document model matches event records well
- easy to query from Streamlit

### 9.2 Cassandra role

Cassandra is used for:

- time-series style reads
- fast access by region and month bucket

Table:

- `disasters.events_by_region_date`

Key design:

- partition key: `(country, date_bucket)`
- clustering order: `event_time DESC, event_id ASC`

Why Cassandra fits:

- optimized for predictable partitioned scans
- very good for "show me recent events for country X in month Y" style access

This is why the Cassandra dashboard page is centered on per-region time-series
analysis rather than free-form filtering.

## 10. Spark Thrift Server and SQL access

File:

- [thrift/start_thrift.ps1](thrift/start_thrift.ps1)

Purpose:

- expose curated HDFS outputs as Spark SQL tables over JDBC

Registered tables include:

- `events_enriched`
- `events_clustered`
- `lead_time_matches`
- `lead_time_summary`
- `cluster_terms`

Why this layer matters:

- it turns HDFS Parquet outputs into SQL-visible datasets
- it powers the SQL Playground page
- it powers the Performance Lab page
- it proves JDBC/ODBC-style access, which is an explicit coursework topic

The Thrift layer is the "warehouse face" of the lakehouse.

## 11. Dashboard explanation

Files:

- [dashboard/app.py](dashboard/app.py)
- [dashboard/pages/](dashboard/pages/)

The dashboard is not one page. It is a collection of focused views, each tied to
one access pattern or project concept.

### 11.1 Home page

Shows health checks for:

- MongoDB
- Cassandra
- Spark Thrift

This makes the dashboard double as an operational control panel.

### 11.2 World Map

File:

- [dashboard/pages/1_World_Map.py](dashboard/pages/1_World_Map.py)

Purpose:

- plot events geographically from MongoDB
- filter by event type, severity, and marker count

Important safety logic:

- coordinates are defensively filtered to valid latitude and longitude ranges

### 11.3 Lead-Time Analysis

File:

- [dashboard/pages/2_Lead_Time.py](dashboard/pages/2_Lead_Time.py)

Purpose:

- visualize the Scala lead-time outputs
- show summary metrics and distribution-style views

### 11.4 MongoDB Explorer

File:

- [dashboard/pages/3_MongoDB_Explorer.py](dashboard/pages/3_MongoDB_Explorer.py)

Purpose:

- browse event documents directly from the serving store
- demonstrate document-database query patterns

### 11.5 Cassandra Time-Series

File:

- [dashboard/pages/4_Cassandra_TimeSeries.py](dashboard/pages/4_Cassandra_TimeSeries.py)

Purpose:

- show why Cassandra was chosen
- query by country and recent month windows

This page is intentionally aligned with the Cassandra data model.

### 11.6 Cluster Themes

File:

- [dashboard/pages/5_Cluster_Themes.py](dashboard/pages/5_Cluster_Themes.py)

Purpose:

- inspect K-means output
- relate clusters to titles and terms

### 11.7 MapReduce Results

File:

- [dashboard/pages/6_MapReduce_Results.py](dashboard/pages/6_MapReduce_Results.py)

Purpose:

- surface the Hadoop keyword-frequency output

### 11.8 Spark SQL Playground

File:

- [dashboard/pages/7_Spark_SQL_Playground.py](dashboard/pages/7_Spark_SQL_Playground.py)

Purpose:

- allow ad hoc SQL over the curated Parquet outputs

This is one of the clearest demonstrations that the project is a usable
analytics system, not only a batch job.

### 11.9 Performance Lab

File:

- [dashboard/pages/8_Performance_Lab.py](dashboard/pages/8_Performance_Lab.py)

Purpose:

- compare cold, cached, and broadcast-hint query execution times

Why it matters:

- it explicitly demonstrates Spark SQL performance tuning concepts

### 11.10 Live Bluesky

File:

- [dashboard/pages/9_Live_Bluesky.py](dashboard/pages/9_Live_Bluesky.py)

Purpose:

- poll Bluesky search
- insert live events into MongoDB
- demonstrate streaming-like operational ingestion at the dashboard layer

Important practical note:

- the public endpoint may return `403`
- authenticated mode using `BLUESKY_HANDLE` and `BLUESKY_APP_PASSWORD` is
  supported in the current repo

### 11.11 Analytics Hub

File:

- [dashboard/pages/10_Analytics_Hub.py](dashboard/pages/10_Analytics_Hub.py)

Purpose:

- provide a single cross-source analytics surface over the lakehouse

Main sections:

- Overview
- Temporal
- Severity
- Geography
- Quality
- Clusters

This page is where the project most clearly becomes an analytics application
rather than just a technical demo.

## 12. UDF layer

Folder:

- [udfs/](udfs/)

The UDF layer carries domain-specific logic that raw Spark transforms do not
provide by default.

Important UDF families:

- classification
- severity scoring
- geoparsing

Why this matters:

- the value of the pipeline is not only moving data around
- it is also adding semantics that were not explicitly present in every source

## 13. Windows-native engineering decisions

This repository currently runs as a Windows-native build, and that matters a
lot. The current implementation is not just a direct copy of the older Linux
and Docker instructions.

### 13.1 Why special handling was needed

Native Windows introduces friction around:

- Hadoop CLI behavior
- Spark commit behavior to HDFS
- Java paths with spaces
- Cassandra compatibility
- Python version compatibility with PySpark

### 13.2 Local stage then HDFS upload

File:

- [pipeline/hdfs_commit.py](pipeline/hdfs_commit.py)

Problem:

- direct Spark Parquet writes to HDFS could hang during the final commit phase
  on this Windows setup

Solution:

- write finished output to a local directory
- upload that directory using `hdfs dfs -put`

Why it is important:

- without this helper, the pipeline is not reliable on Windows
- this is one of the most important repository-specific engineering fixes

### 13.3 Plain subdirectories instead of Hive-style partitions

Problem:

- Windows Hadoop CLI can mis-handle paths containing `=`

Solution:

- write source outputs as plain subdirectories
- read them later using `recursiveFileLookup=true`

Impact:

- partition discovery is no longer relied upon
- the `source` column inside the data remains authoritative

### 13.4 Python environment split

The current repo effectively uses different Python environments for different
roles:

- `.venv`
  General app/dashboard environment.

- `.venv-spark311`
  Spark-specific environment pinned to Python 3.11 because PySpark on Windows
  behaved poorly with the main Python 3.12 environment.

- `.venv-ingest`
  Lightweight downloader environment.

This split is a practical stability decision, not accidental complexity.

### 13.5 Java path simplification

Java is referenced through:

- `C:\jdk11`

This avoids repeated failures caused by spaces in `Program Files`.

### 13.6 Project-local HDFS storage

HDFS storage lives under:

- [.hdfs/](.hdfs/)

Why:

- this avoids Windows permission problems that occurred with broader system
  directories

### 13.7 Cassandra compatibility adjustments

The current Windows-native Cassandra setup required:

- using a Windows-capable Cassandra version
- JVM option cleanup for Java 11 compatibility
- avoiding problematic native metrics libraries

That work is part of what made the whole stack practical on Windows.

## 14. Performance and scale characteristics

This project can run in both smaller and larger profiles, but the main scaling
story is straightforward:

- GDELT dominates row counts and runtime
- text-heavy enrichment is the most expensive logical stage
- clustering is the most expensive ML stage
- serving-store writes can be very large in full profiles

Design choices that help:

- sampling during model fit in step 5
- classifier/geoparse short-circuiting in step 2
- per-source writes in steps 1 and 2
- separate output file counts for heavy sources such as GDELT
- Spark SQL caching and broadcast hints in the performance page

## 15. What the project demonstrates academically

If someone asks "what are the course concepts shown here?", the answer is:

- HDFS as a distributed storage layer
- Spark SQL for transformation and querying
- PySpark applications
- user-defined functions
- MapReduce as a separate batch model
- Scala Spark in a dedicated analytical job
- MongoDB as a document serving layer
- Cassandra as a time-series serving layer
- JDBC/ODBC access through Spark Thrift
- MLlib clustering
- performance tuning concepts such as caching and broadcast hints

This is one of the strongest aspects of the project: it is not just technically
busy, it is well aligned with the course outline.

## 16. Known caveats and tradeoffs

The project is strong, but not magic. A few caveats matter.

### 16.1 README mismatch

The top-level [README.md](README.md) still contains older Linux/Docker-oriented
material, while the actual working setup in this repo is Windows-native.

This file is therefore the better explanation of the current repo behavior.

### 16.2 GPU skip mode changes semantics

When `SKIP_GPU=1` is enabled:

- classification quality becomes more limited
- some lead-time comparisons disappear
- "working pipeline" does not necessarily mean "full semantic richness"

### 16.3 Full GDELT is expensive

With a full download profile:

- GDELT can dominate total rows
- full reruns become much slower and more memory-sensitive

### 16.4 Live Bluesky is operationally separate

The live page is valuable, but it is not the same as the batch historical
pipeline. It is a live add-on feeding MongoDB.

## 17. How to explain the project in one minute

If you need a short viva-style summary:

"This project is a disaster-intelligence lakehouse that ingests multiple public
disaster sources into HDFS, normalizes them into a shared schema, enriches them
with classification, severity, and geoparsing, runs MapReduce, Scala Spark, and
clustering jobs, then serves the results through MongoDB, Cassandra, Spark SQL
Thrift, and a Streamlit dashboard. It demonstrates storage, processing,
serving, SQL access, analytics, and performance tuning in one end-to-end
system."

## 18. How to explain the value of each layer

If asked why so many layers exist:

- local raw files preserve original downloads
- HDFS gives a shared batch storage layer
- normalized Parquet gives one schema
- enriched Parquet gives analytics-ready features
- MR and Scala outputs prove multiple compute paradigms
- MongoDB and Cassandra support different serving patterns
- Thrift gives SQL access
- Streamlit turns the whole system into something inspectable and explainable

Each layer exists for a reason; it is not redundant.

## 19. Suggested reading order if you want to understand the code

1. [README.md](README.md)
2. [run_pipeline.ps1](run_pipeline.ps1)
3. [pipeline/01_clean_normalize.py](pipeline/01_clean_normalize.py)
4. [pipeline/02_enrich.py](pipeline/02_enrich.py)
5. [pipeline/06_load_serving_stores.py](pipeline/06_load_serving_stores.py)
6. [thrift/start_thrift.ps1](thrift/start_thrift.ps1)
7. [dashboard/app.py](dashboard/app.py)
8. [dashboard/pages/10_Analytics_Hub.py](dashboard/pages/10_Analytics_Hub.py)

That order gives the fastest path from architecture to implementation details.

## 20. Final takeaway

The strongest way to think about this repository is:

- not as a single script
- not as only a dashboard
- not as only a coursework checklist

It is a full pipeline-plus-serving-plus-analytics system where each technology
was chosen to illustrate a different role:

- HDFS for storage
- Spark for transformation
- MapReduce and Scala for alternative analytics jobs
- MongoDB and Cassandra for different serving patterns
- Thrift for SQL access
- Streamlit for explanation and interaction

That is why the project is compelling both technically and academically.
