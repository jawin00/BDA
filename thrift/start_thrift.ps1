# Start the Spark SQL Thrift Server with enriched Parquet tables pre-registered.
$ErrorActionPreference = "Stop"

if (-not $env:SPARK_HOME)  { $env:SPARK_HOME  = "C:\spark" }
if (-not $env:HADOOP_HOME) { $env:HADOOP_HOME = "C:\hadoop" }
if (-not $env:JAVA_HOME)   { $env:JAVA_HOME   = "C:\jdk11" }
if (-not $env:HADOOP_CONF_DIR) { $env:HADOOP_CONF_DIR = "$env:HADOOP_HOME\etc\hadoop" }
$env:Path = "$env:JAVA_HOME\bin;$env:SPARK_HOME\bin;$env:SPARK_HOME\sbin;$env:HADOOP_HOME\bin;$env:Path"

$initSql = "$env:TEMP\thrift_init.sql"
$probeSql = "$env:TEMP\thrift_probe.sql"

$sql = @"
DROP TABLE IF EXISTS events_enriched;
CREATE TABLE events_enriched USING parquet
  OPTIONS (
    path 'hdfs://localhost:9000/processed/events_enriched',
    recursiveFileLookup 'true'
  );

DROP TABLE IF EXISTS events_clustered;
CREATE TABLE events_clustered USING parquet
  OPTIONS (
    path 'hdfs://localhost:9000/processed/events_clustered',
    recursiveFileLookup 'true'
  );

DROP TABLE IF EXISTS lead_time_matches;
CREATE TABLE lead_time_matches USING parquet
  OPTIONS (
    path 'hdfs://localhost:9000/processed/lead_time/matches',
    recursiveFileLookup 'true'
  );

DROP TABLE IF EXISTS lead_time_summary;
CREATE TABLE lead_time_summary USING parquet
  OPTIONS (
    path 'hdfs://localhost:9000/processed/lead_time/summary',
    recursiveFileLookup 'true'
  );

DROP TABLE IF EXISTS cluster_terms;
CREATE TABLE cluster_terms USING parquet
  OPTIONS (
    path 'hdfs://localhost:9000/processed/cluster_terms',
    recursiveFileLookup 'true'
  );
"@
$sql | Set-Content -Encoding ASCII $initSql
"SHOW DATABASES;" | Set-Content -Encoding ASCII $probeSql

$logDir = "$env:SPARK_HOME\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Invoke-Beeline {
    param(
        [string[]] $BeelineArgs,
        [string] $OutFile,
        [string] $ErrFile
    )

    $proc = Start-Process -FilePath "$env:SPARK_HOME\bin\beeline.cmd" `
        -ArgumentList $BeelineArgs `
        -Wait `
        -PassThru `
        -RedirectStandardOutput $OutFile `
        -RedirectStandardError $ErrFile `
        -WindowStyle Hidden
    return $proc.ExitCode
}

$thriftJar = (Get-ChildItem "$env:SPARK_HOME\jars" -Filter "spark-hive-thriftserver*.jar" | Select-Object -First 1)
if (-not $thriftJar) {
    Write-Warning "[thrift] spark-hive-thriftserver jar not found - ensure Spark is built with Hive support."
}

$listening = Get-NetTCPConnection -LocalPort 10000 -State Listen -ErrorAction SilentlyContinue
if ($listening) {
    Write-Host "[thrift] server already listening on port 10000"
} else {
    Write-Host "[thrift] starting Spark Thrift Server"
    Start-Process -FilePath "$env:SPARK_HOME\bin\spark-submit.cmd" `
        -ArgumentList @(
            "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
            "--master", "local[2]",
            "--conf", "spark.sql.warehouse.dir=/user/hive/warehouse",
            "--conf", "spark.sql.adaptive.enabled=true",
            "--conf", "spark.driver.bindAddress=127.0.0.1",
            "--conf", "spark.driver.host=127.0.0.1",
            "--conf", "spark.local.dir=C:\tmp\spark",
            "--hiveconf", "hive.server2.thrift.port=10000",
            "--hiveconf", "hive.server2.thrift.bind.host=0.0.0.0",
            "--conf", "spark.hadoop.fs.defaultFS=hdfs://localhost:9000"
        ) `
        -RedirectStandardOutput "$logDir\thrift.out" `
        -RedirectStandardError  "$logDir\thrift.err" `
        -WindowStyle Hidden
}

Write-Host "[thrift] waiting for server (up to 120 s)..."
$ready = $false
for ($i = 1; $i -le 60; $i++) {
    $code = Invoke-Beeline `
        -BeelineArgs @("-u", "jdbc:hive2://localhost:10000", "-f", $probeSql) `
        -OutFile "$logDir\beeline_probe.out" `
        -ErrFile "$logDir\beeline_probe.err"
    if ($code -eq 0) { $ready = $true; break }
    Start-Sleep -Seconds 2
}

if (-not $ready) {
    Write-Warning "[thrift] server did not become ready in time - check $logDir\thrift.err"
} else {
    Write-Host "[thrift] registering views"
    $code = Invoke-Beeline `
        -BeelineArgs @("-u", "jdbc:hive2://localhost:10000", "-f", $initSql) `
        -OutFile "$logDir\beeline_init.out" `
        -ErrFile "$logDir\beeline_init.err"
    if ($code -ne 0) {
        Write-Warning "[thrift] view init failed (non-fatal - pipeline may not have run yet)"
        Get-Content "$logDir\beeline_init.err" -Tail 20
    }
    Write-Host "[thrift] ready on jdbc:hive2://localhost:10000"
}
