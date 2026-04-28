# Push everything under .\data\raw\ into HDFS at /raw/<source>/
# Idempotent: skips a source if its HDFS dir is already non-empty.
$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot\.."

if (-not $env:HADOOP_HOME) { $env:HADOOP_HOME = "C:\hadoop" }
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"

function Invoke-Hdfs {
    & hdfs dfs @args
}

Write-Host "[load_to_hdfs] checking HDFS NameNode"
hdfs dfsadmin -safemode get | Out-Null

Write-Host "[load_to_hdfs] creating HDFS base dirs"
foreach ($dir in @("/raw", "/processed", "/mr_out", "/mr_in", "/incoming")) {
    hdfs dfs -mkdir -p $dir 2>$null
}

function Push-ToHdfs {
    param($src, $dst)
    $localDir = "data\raw\$src"

    if (-not (Test-Path $localDir) -or (Get-ChildItem $localDir -Force -ErrorAction SilentlyContinue | Measure-Object).Count -eq 0) {
        Write-Host "[load_to_hdfs] SKIP ${src}: no local files"
        return
    }

    $count = 0
    $existing = cmd /c "hdfs dfs -ls /raw/$dst 2>nul"
    if ($LASTEXITCODE -eq 0) {
        $count = ($existing | Where-Object { $_ -match '^-' } | Measure-Object).Count
    }
    if ($count -gt 0) {
        Write-Host "[load_to_hdfs] SKIP ${src}: /raw/${dst} already has ${count} files"
        return
    }

    Write-Host "[load_to_hdfs] put ${src} -> /raw/${dst}"
    hdfs dfs -mkdir -p "/raw/$dst"
    Get-ChildItem $localDir -Force | ForEach-Object {
        hdfs dfs -put -f $_.FullName "/raw/$dst/"
        if ($LASTEXITCODE -ne 0) { throw "failed to upload $($_.FullName) to /raw/$dst" }
    }
    $sizeMB = [math]::Round((Get-ChildItem $localDir -Recurse -File | Measure-Object -Property Length -Sum).Sum / 1MB, 1)
    Write-Host "[load_to_hdfs]   done: ${sizeMB} MB"
}

Push-ToHdfs usgs      usgs
Push-ToHdfs eonet     eonet
Push-ToHdfs reliefweb reliefweb
Push-ToHdfs gdacs     gdacs
Push-ToHdfs gdelt     gdelt
Push-ToHdfs social    social

Write-Host ""
Write-Host "[load_to_hdfs] HDFS /raw layout:"
hdfs dfs -du -h /raw
