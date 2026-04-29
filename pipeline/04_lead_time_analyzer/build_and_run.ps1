# Build the Scala fat jar with sbt, then submit it to Spark.
$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot\..\.."

if (-not $env:HADOOP_HOME) { $env:HADOOP_HOME = "C:\hadoop" }
if (-not $env:SPARK_HOME)  { $env:SPARK_HOME  = "C:\spark" }
if (-not $env:JAVA_HOME)   { $env:JAVA_HOME   = "C:\jdk11" }
$env:Path = "$env:SPARK_HOME\bin;$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:Path"
$repoRoot = Resolve-Path "$PSScriptRoot\..\.."
$sparkLocalDir = Join-Path $env:LOCALAPPDATA "Temp\bda_spark_local"
New-Item -ItemType Directory -Force -Path $sparkLocalDir | Out-Null

$jobDir = "pipeline\04_lead_time_analyzer"
$stageRoot = Join-Path $env:LOCALAPPDATA "Temp\bda_spark_stage\lead_time"
if (Test-Path $stageRoot) { Remove-Item -LiteralPath $stageRoot -Recurse -Force }
New-Item -ItemType Directory -Force -Path $stageRoot | Out-Null
$env:BDA_LEAD_TIME_STAGE = $stageRoot

Write-Host "[04] building Scala fat jar with sbt"
Push-Location $jobDir
& sbt -batch assembly
if ($LASTEXITCODE -ne 0) { throw "sbt assembly failed" }
Pop-Location

$jar = "$jobDir\target\scala-2.12\lead-time-analyzer-assembly.jar"
if (-not (Test-Path $jar)) { throw "[04] jar not found: $jar" }

Write-Host "[04] submitting Scala Spark job"
& spark-submit `
    --master "local[2]" `
    --conf "spark.hadoop.fs.defaultFS=hdfs://localhost:9000" `
    --conf "spark.driver.memory=2g" `
    --conf "spark.driver.bindAddress=127.0.0.1" `
    --conf "spark.driver.host=127.0.0.1" `
    --conf "spark.local.dir=$sparkLocalDir" `
    --class edu.bda.LeadTimeAnalyzer `
    $jar
if ($LASTEXITCODE -ne 0) { throw "Spark job failed" }

Write-Host "[04] uploading local parquet output to HDFS"
hdfs dfs -rm -r -f /processed/lead_time 2>$null
hdfs dfs -mkdir -p /processed/lead_time
hdfs dfs -put "$stageRoot\matches" /processed/lead_time/matches
if ($LASTEXITCODE -ne 0) { throw "failed to upload lead_time matches" }
hdfs dfs -put "$stageRoot\summary" /processed/lead_time/summary
if ($LASTEXITCODE -ne 0) { throw "failed to upload lead_time summary" }
