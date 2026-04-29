# Build the Java MapReduce jar with Maven, then submit it to Hadoop.
$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot\..\.."

if (-not $env:HADOOP_HOME) { $env:HADOOP_HOME = "C:\hadoop" }
if (-not $env:SPARK_HOME)  { $env:SPARK_HOME  = "C:\spark" }
if (-not $env:JAVA_HOME)   { $env:JAVA_HOME   = "C:\jdk11" }
$env:Path = "$env:SPARK_HOME\bin;$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:Path"

$repoRoot = Resolve-Path "$PSScriptRoot\..\.."
$sparkLocalDir = Join-Path $env:LOCALAPPDATA "Temp\bda_spark_local"
New-Item -ItemType Directory -Force -Path $sparkLocalDir | Out-Null
function Resolve-WorkingVenv {
    $candidates = @()
    if ($env:VENV) { $candidates += $env:VENV }
    $candidates += "$repoRoot\.venv-spark311"
    $candidates += "$repoRoot\.venv"

    foreach ($venvPath in ($candidates | Select-Object -Unique)) {
        $py = Join-Path $venvPath "Scripts\python.exe"
        if (-not (Test-Path $py)) { continue }
        try {
            & $py -c "import sys; print(sys.executable)" *> $null
            if ($LASTEXITCODE -eq 0) { return $venvPath }
        } catch {
        }
        Write-Warning "[03] ignoring broken python launcher: $py"
    }

    throw "[03] no working virtualenv python found"
}

$venv   = Resolve-WorkingVenv
$python = "$venv\Scripts\python.exe"

$input  = "hdfs://localhost:9000/mr_in/keyword_freq"
$output = "hdfs://localhost:9000/mr_out/keyword_freq"

Write-Host "[03] staging TSV input via Spark"
$env:PYSPARK_PYTHON = $python
$env:PYSPARK_DRIVER_PYTHON = $python
$env:PYTHONPATH = "$repoRoot"
$env:Path = "$venv\Scripts;$env:Path"
& spark-submit `
    --master "local[2]" `
    --conf "spark.hadoop.fs.defaultFS=hdfs://localhost:9000" `
    --conf "spark.pyspark.python=$python" `
    --conf "spark.pyspark.driver.python=$python" `
    --conf "spark.driver.memory=2g" `
    --conf "spark.local.dir=$sparkLocalDir" `
    pipeline\03_keyword_freq_mr\stage_input.py
if ($LASTEXITCODE -ne 0) { throw "stage_input.py failed" }

Write-Host "[03] building MR jar with Maven"
& mvn -q -DskipTests package -f pipeline\03_keyword_freq_mr\pom.xml
if ($LASTEXITCODE -ne 0) { throw "Maven build failed" }

$jar = "pipeline\03_keyword_freq_mr\target\keyword-freq-mr.jar"
if (-not (Test-Path $jar)) { throw "[03] jar not found: $jar" }

Write-Host "[03] clearing previous MR output"
hdfs dfs -rm -r -f -skipTrash $output 2>$null

Write-Host "[03] submitting MapReduce job"
$env:HADOOP_CLASSPATH = $jar
& hadoop jar $jar edu.bda.mr.KeywordFreqMR $input $output
if ($LASTEXITCODE -ne 0) { throw "MapReduce job failed" }

Write-Host "[03] sample output:"
hdfs dfs -cat "$output/part-r-00000" 2>$null | Select-Object -First 20
