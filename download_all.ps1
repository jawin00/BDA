# Fetch all historical disaster sources into .\data\raw\
# Honors DOWNLOAD_PROFILE=small in .env for a 10x smaller subset.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# Load .env if present
if (Test-Path ".env") {
    Get-Content ".env" | Where-Object { $_ -notmatch '^\s*#' -and $_ -match '=' } | ForEach-Object {
        $parts = $_ -split '=', 2
        [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), 'Process')
    }
}

$profile = if ($env:DOWNLOAD_PROFILE) { $env:DOWNLOAD_PROFILE } else { "full" }
$smallFlag = ""
if ($profile -eq "small") {
    $smallFlag = "--small"
    Write-Host "[download_all] DOWNLOAD_PROFILE=small — fetching reduced subset"
} else {
    Write-Host "[download_all] DOWNLOAD_PROFILE=full — fetching ~7 GB"
}

# Create a local venv for downloaders (no Spark needed for this step)
if (-not (Test-Path ".venv-ingest")) {
    Write-Host "[download_all] creating .venv-ingest for downloader deps"
    python -m venv .venv-ingest
    & .\.venv-ingest\Scripts\pip install --quiet --upgrade pip
    & .\.venv-ingest\Scripts\pip install --quiet requests tqdm
}
$py = ".\.venv-ingest\Scripts\python.exe"

Write-Host "[download_all] 1/6 USGS earthquakes"
& $py ingest\download_usgs.py $smallFlag

Write-Host "[download_all] 2/6 NASA EONET"
& $py ingest\download_eonet.py $smallFlag

Write-Host "[download_all] 3/6 ReliefWeb reports"
& $py ingest\download_reliefweb.py $smallFlag

Write-Host "[download_all] 4/6 GDACS alerts"
& $py ingest\download_gdacs.py $smallFlag

Write-Host "[download_all] 5/6 GDELT events (filtered)"
& $py ingest\download_gdelt.py $smallFlag

Write-Host "[download_all] 6/6 HumAID labeled tweets"
& $py ingest\download_humaid.py $smallFlag

Write-Host "[download_all] bonus: GeoNames cities500 lexicon"
& $py ingest\download_geonames.py

Write-Host ""
Write-Host "[download_all] done. Sizes:"
if (Test-Path "data\raw") {
    Get-ChildItem "data\raw" | ForEach-Object {
        $size = (Get-ChildItem $_.FullName -Recurse -File | Measure-Object -Property Length -Sum).Sum
        "{0,8:N0} KB  {1}" -f ($size / 1KB), $_.Name
    }
}
Write-Host ""
Write-Host "Next: .\ingest\load_to_hdfs.ps1"
