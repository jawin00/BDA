# Extract Hadoop + Spark, configure HDFS, and start services.
# Run once after downloads are complete.
$ErrorActionPreference = "Stop"

$JAVA_HOME   = "C:\jdk11"
$HADOOP_HOME = "C:\hadoop"
$SPARK_HOME  = "C:\spark"

$env:JAVA_HOME   = $JAVA_HOME
$env:HADOOP_HOME = $HADOOP_HOME
$env:SPARK_HOME  = $SPARK_HOME
$env:Path = "$JAVA_HOME\bin;$HADOOP_HOME\bin;$SPARK_HOME\bin;$env:Path"

# ── Extract Hadoop ────────────────────────────────────────────────────────────
$hadoopTar = "$env:TEMP\hadoop-3.3.6.tar.gz"
if (-not (Test-Path $hadoopTar)) { throw "Hadoop tarball not found at $hadoopTar — download it first" }
if (-not (Test-Path "$HADOOP_HOME\bin\hadoop.cmd")) {
    Write-Host "[setup] extracting Hadoop to $HADOOP_HOME ..."
    & "$JAVA_HOME\bin\jar.exe" xf $hadoopTar -C "C:\" 2>$null
    # tar is available on Windows 10+
    tar -xzf $hadoopTar -C "C:\" 2>&1 | Select-Object -Last 3
    Rename-Item -Path "C:\hadoop-3.3.6" -NewName "hadoop" -ErrorAction SilentlyContinue
    Write-Host "[setup] Hadoop extracted"
} else {
    Write-Host "[setup] Hadoop already extracted"
}

# Copy winutils
if (Test-Path "C:\hadoop-winutils\winutils.exe") {
    Copy-Item "C:\hadoop-winutils\winutils.exe" "$HADOOP_HOME\bin\" -Force
    Copy-Item "C:\hadoop-winutils\hadoop.dll"   "$HADOOP_HOME\bin\" -Force
    Write-Host "[setup] winutils copied"
}

# ── Hadoop config ─────────────────────────────────────────────────────────────
$confDir = "$HADOOP_HOME\etc\hadoop"

@"
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
</configuration>
"@ | Set-Content "$confDir\core-site.xml" -Encoding UTF8

$hdfsRoot = "$PSScriptRoot\.hdfs"
New-Item -ItemType Directory -Force -Path "$hdfsRoot\namenode" | Out-Null
New-Item -ItemType Directory -Force -Path "$hdfsRoot\datanode"  | Out-Null

@"
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///$($hdfsRoot.Replace('\','/'))/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///$($hdfsRoot.Replace('\','/'))/datanode</value></property>
</configuration>
"@ | Set-Content "$confDir\hdfs-site.xml" -Encoding UTF8

# Set JAVA_HOME in hadoop-env.cmd
$envFile = "$confDir\hadoop-env.cmd"
if (Test-Path $envFile) {
    $content = Get-Content $envFile -Raw
    if ($content -notmatch 'set JAVA_HOME=C:\\Program Files') {
        "set JAVA_HOME=$JAVA_HOME" | Add-Content $envFile
    }
} else {
    "set JAVA_HOME=$JAVA_HOME" | Set-Content $envFile
}

Write-Host "[setup] Hadoop configured"

# ── Format HDFS (only if namenode is empty) ───────────────────────────────────
if ((Get-ChildItem "$hdfsRoot\namenode" -ErrorAction SilentlyContinue | Measure-Object).Count -eq 0) {
    Write-Host "[setup] formatting HDFS namenode..."
    & "$HADOOP_HOME\bin\hdfs.cmd" namenode -format -nonInteractive 2>&1 | Select-Object -Last 5
    Write-Host "[setup] HDFS formatted"
} else {
    Write-Host "[setup] HDFS already formatted"
}

# ── Start HDFS ────────────────────────────────────────────────────────────────
Write-Host "[setup] starting HDFS (NameNode + DataNode)..."
Start-Process -FilePath "$HADOOP_HOME\sbin\start-dfs.cmd" -NoNewWindow -Wait

Start-Sleep -Seconds 10

# Verify
$check = & "$HADOOP_HOME\bin\hdfs.cmd" dfsadmin -safemode get 2>&1
Write-Host "[setup] HDFS status: $check"

# ── Extract Spark ─────────────────────────────────────────────────────────────
$sparkTar = "$env:TEMP\spark-3.5.4-bin-hadoop3.tgz"
if (-not (Test-Path $sparkTar)) { throw "Spark tarball not found at $sparkTar" }
if (-not (Test-Path "$SPARK_HOME\bin\spark-submit.cmd")) {
    Write-Host "[setup] extracting Spark to $SPARK_HOME ..."
    tar -xzf $sparkTar -C "C:\" 2>&1 | Select-Object -Last 3
    Rename-Item -Path "C:\spark-3.5.4-bin-hadoop3" -NewName "spark" -ErrorAction SilentlyContinue
    Write-Host "[setup] Spark extracted"
} else {
    Write-Host "[setup] Spark already extracted"
}

# ── Start MongoDB ─────────────────────────────────────────────────────────────
Write-Host "[setup] starting MongoDB..."
Start-Service -Name "MongoDB" -ErrorAction SilentlyContinue
$mongo = Get-Service -Name "MongoDB" -ErrorAction SilentlyContinue
if ($mongo) { Write-Host "[setup] MongoDB: $($mongo.Status)" } else { Write-Host "[setup] MongoDB service not found" }

Write-Host ""
Write-Host "[setup] DONE. Next steps:"
Write-Host "  1. Load data into HDFS:  .\ingest\load_to_hdfs.ps1"
Write-Host "  2. Run pipeline:         .\run_pipeline.ps1"
