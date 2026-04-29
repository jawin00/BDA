# Optional: start the Spark Structured Streaming file-source job.
# Drop files into .\data\incoming\ to trigger processing.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

New-Item -ItemType Directory -Force -Path "data\incoming" | Out-Null
New-Item -ItemType Directory -Force -Path "data\streaming_checkpoint" | Out-Null

if (-not $env:HADOOP_HOME) { $env:HADOOP_HOME = "C:\hadoop" }
if (-not $env:SPARK_HOME)  { $env:SPARK_HOME  = "C:\spark" }
if (-not $env:JAVA_HOME)   { $env:JAVA_HOME   = "C:\jdk11" }
$env:HADOOP_CONF_DIR = "$env:HADOOP_HOME\etc\hadoop"
$env:Path = "$env:SPARK_HOME\bin;$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:Path"
$sparkLocalDir = Join-Path $env:LOCALAPPDATA "Temp\bda_spark_local"
$sparkStageDir = Join-Path $env:LOCALAPPDATA "Temp\bda_spark_stage"
New-Item -ItemType Directory -Force -Path $sparkLocalDir, $sparkStageDir | Out-Null
$env:BDA_SPARK_STAGE = $sparkStageDir

function Resolve-WorkingVenv {
    $candidates = @()
    if ($env:VENV) { $candidates += $env:VENV }
    $candidates += "$PSScriptRoot\.venv-spark311"
    $candidates += "$PSScriptRoot\.venv"

    foreach ($venvPath in ($candidates | Select-Object -Unique)) {
        $py = Join-Path $venvPath "Scripts\python.exe"
        if (-not (Test-Path $py)) { continue }
        try {
            & $py -c "import sys; print(sys.executable)" *> $null
            if ($LASTEXITCODE -eq 0) { return $venvPath }
        } catch {
        }
        Write-Warning "[streaming] ignoring broken python launcher: $py"
    }

    throw "[streaming] no working virtualenv python found"
}

$venv   = Resolve-WorkingVenv
$python = "$venv\Scripts\python.exe"
$env:PYSPARK_PYTHON = $python
$env:PYSPARK_DRIVER_PYTHON = $python
$env:Path = "$venv\Scripts;$env:Path"

Write-Host "[streaming] starting Spark Structured Streaming on data\incoming\"
Write-Host "[streaming] In another terminal:  Copy-Item data\samples\sample_event.json data\incoming\"
Write-Host "[streaming] Ctrl+C to stop."

& spark-submit `
    --master "local[2]" `
    --conf "spark.hadoop.fs.defaultFS=hdfs://localhost:9000" `
    --conf "spark.pyspark.python=$python" `
    --conf "spark.pyspark.driver.python=$python" `
    --conf "spark.driver.memory=2g" `
    --conf "spark.executor.memory=2g" `
    --conf "spark.driver.bindAddress=127.0.0.1" `
    --conf "spark.driver.host=127.0.0.1" `
    --conf "spark.local.dir=$sparkLocalDir" `
    --conf "spark.sql.shuffle.partitions=4" `
    --packages "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0" `
    pipeline\streaming\file_source_stream.py

if ($LASTEXITCODE -ne 0) { throw "Streaming job exited with code $LASTEXITCODE" }
