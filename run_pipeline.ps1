# Run all 6 pipeline steps natively (no Docker).
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# Load .env if present
if (Test-Path ".env") {
    Get-Content ".env" | Where-Object { $_ -notmatch '^\s*#' -and $_ -match '=' } | ForEach-Object {
        $parts = $_ -split '=', 2
        [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), 'Process')
    }
}

if (-not $env:HADOOP_HOME) { $env:HADOOP_HOME = "C:\hadoop" }
if (-not $env:SPARK_HOME)  { $env:SPARK_HOME  = "C:\spark" }
if (-not $env:JAVA_HOME)   { $env:JAVA_HOME   = "C:\jdk11" }
$env:HADOOP_CONF_DIR = "$env:HADOOP_HOME\etc\hadoop"
$env:Path = "$env:SPARK_HOME\bin;$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:Path"

$skipGpu = if ($env:SKIP_GPU) { $env:SKIP_GPU } else { "0" }
$defaultVenv = if (Test-Path "$PSScriptRoot\.venv-spark311\Scripts\python.exe") { "$PSScriptRoot\.venv-spark311" } else { "$PSScriptRoot\.venv" }
$venv    = if ($env:VENV) { $env:VENV } else { $defaultVenv }
$python  = "$venv\Scripts\python.exe"

Write-Host "==[run_pipeline] SKIP_GPU=$skipGpu"

$connectors = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"

function Invoke-Spark {
    param([string]$script, [string[]]$extraArgs = @())
    $env:PYSPARK_PYTHON = $python
    $env:PYSPARK_DRIVER_PYTHON = $python
    $env:Path = "$venv\Scripts;$env:SPARK_HOME\bin;$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:Path"
    $env:SKIP_GPU = $skipGpu
    & spark-submit `
        --master "local[2]" `
        --conf "spark.hadoop.fs.defaultFS=hdfs://localhost:9000" `
        --conf "spark.pyspark.python=$python" `
        --conf "spark.pyspark.driver.python=$python" `
        --conf "spark.driver.memory=2g" `
        --conf "spark.executor.memory=2g" `
        --conf "spark.driver.bindAddress=127.0.0.1" `
        --conf "spark.driver.host=127.0.0.1" `
        --conf "spark.local.dir=C:\tmp\spark" `
        --conf "spark.sql.shuffle.partitions=4" `
        --conf "spark.sql.adaptive.enabled=true" `
        --conf "spark.driver.extraJavaOptions=-XX:ReservedCodeCacheSize=64m -XX:+TieredCompilation -XX:TieredStopAtLevel=1 -Xss512k" `
        --conf "spark.executor.extraJavaOptions=-XX:ReservedCodeCacheSize=64m -XX:+TieredCompilation -XX:TieredStopAtLevel=1 -Xss512k" `
        @extraArgs `
        $script
    if ($LASTEXITCODE -ne 0) { throw "spark-submit failed for $script" }
}

# 0. Load data into HDFS
Write-Host ""
Write-Host "==[run_pipeline] step 0: HDFS load"
& "$PSScriptRoot\ingest\load_to_hdfs.ps1"

# 1. Clean + normalize
Write-Host ""
Write-Host "==[run_pipeline] step 1: clean + normalize"
Invoke-Spark "pipeline\01_clean_normalize.py"

$count = (hdfs dfs -du -s /processed/events_raw/* 2>$null | Measure-Object).Count
if ($count -eq 0) { throw "step 1 produced no output" }

# 2. Enrich
Write-Host ""
Write-Host "==[run_pipeline] step 2: enrich (SKIP_GPU=$skipGpu)"
Invoke-Spark "pipeline\02_enrich.py"

# 3. MapReduce keyword frequency
Write-Host ""
Write-Host "==[run_pipeline] step 3: MapReduce"
& "$PSScriptRoot\pipeline\03_keyword_freq_mr\build_and_run.ps1"

# 4. Scala lead-time analyzer
Write-Host ""
Write-Host "==[run_pipeline] step 4: Scala job"
& "$PSScriptRoot\pipeline\04_lead_time_analyzer\build_and_run.ps1"

# 5. Cluster themes
Write-Host ""
Write-Host "==[run_pipeline] step 5: cluster themes"
Invoke-Spark "pipeline\05_cluster_themes.py"

# 6. Load to MongoDB + Cassandra
Write-Host ""
Write-Host "==[run_pipeline] step 6: load serving stores"
Invoke-Spark "pipeline\06_load_serving_stores.py" @("--packages", $connectors)

Write-Host ""
Write-Host "==[run_pipeline] all done"
Write-Host "Start Thrift Server: .\thrift\start_thrift.ps1"
Write-Host "Start dashboard:     $venv\Scripts\streamlit run dashboard\app.py"
