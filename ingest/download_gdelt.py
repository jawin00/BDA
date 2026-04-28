#!/usr/bin/env python3
"""GDELT 2.0 events downloader — full pull.

Downloads ALL 96 fifteen-minute export files per day (no CAMEO filter).
Default: 90 days → ~3.4 GB uncompressed, ~560 MB download.
--small: 7 days, 4 slots/day → ~11 MB uncompressed, ~18 MB download.

Output: data/raw/gdelt/gdelt_YYYY-MM-DD.csv  (one file per day, all slots cat'd)
"""

import argparse
import datetime as dt
import io
import sys
import time
import zipfile
from pathlib import Path

import requests
from tqdm import tqdm

GDELT_BASE = "http://data.gdeltproject.org/gdeltv2"


def slots_for_date(d: dt.date, every_n: int = 1) -> list[str]:
    urls = []
    for slot in range(0, 96, every_n):
        total_mins = slot * 15
        hh, mm = divmod(total_mins, 60)
        ts = f"{d.strftime('%Y%m%d')}{hh:02d}{mm:02d}00"
        urls.append(f"{GDELT_BASE}/{ts}.export.CSV.zip")
    return urls


def fetch_zip(url: str) -> bytes | None:
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 200:
                return r.content
            if r.status_code == 404:
                return None
            time.sleep(5 * (attempt + 1))
        except requests.RequestException:
            time.sleep(5 * (attempt + 1))
    return None


def extract_csv(raw: bytes) -> bytes:
    z = zipfile.ZipFile(io.BytesIO(raw))
    with z.open(z.namelist()[0]) as f:
        return f.read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/gdelt")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--small", action="store_true",
                    help="7 days, 4 slots/day (~11 MB) for quick pipeline testing")
    args = ap.parse_args()

    if args.small:
        days = 7
        every_n = 24  # one slot per 6 hours → 4 slots/day
    else:
        days = args.days
        every_n = 1   # all 96 slots/day

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    end = dt.date.today() - dt.timedelta(days=1)
    start = end - dt.timedelta(days=days)
    dates = [start + dt.timedelta(days=i) for i in range(days)]

    slots_per_day = 96 // every_n
    print(f"GDELT: {days} days × {slots_per_day} slots/day = {days * slots_per_day} files")
    print(f"Expected uncompressed: ~{days * slots_per_day * 391 / 1024:.1f} MB")

    total_rows = 0
    for d in tqdm(dates, desc="GDELT days"):
        target = out_dir / f"gdelt_{d.isoformat()}.csv"
        if target.exists() and target.stat().st_size > 10_000:
            continue
        day_chunks = []
        for url in slots_for_date(d, every_n=every_n):
            raw = fetch_zip(url)
            if raw is None:
                continue
            day_chunks.append(extract_csv(raw))
            time.sleep(0.1)
        if day_chunks:
            combined = b"".join(day_chunks)
            target.write_bytes(combined)
            total_rows += combined.count(b"\n")

    total_mb = sum(p.stat().st_size for p in out_dir.glob("gdelt_*.csv")) / 1e6
    print(f"GDELT done: ~{total_rows:,} rows, {total_mb:.1f} MB on disk")


if __name__ == "__main__":
    main()
