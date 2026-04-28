#!/usr/bin/env python3
"""USGS Earthquake Catalog downloader.

Pulls 5 years of M2.5+ events from the FDSN web service in monthly chunks.
M2.5+ gives ~6,000-10,000 events/month globally — well under the API's
20,000-row cap per query.
Default: 5 years, M2.5+ → ~260 MB.
--small: 6 months, M4.5+ → ~13 MB.

Output: one GeoJSON file per month under data/raw/usgs/.
"""

import argparse
import datetime as dt
import sys
import time
from pathlib import Path

import requests
from tqdm import tqdm

USGS_FDSN = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def month_ranges(start: dt.date, end: dt.date):
    cur = dt.date(start.year, start.month, 1)
    while cur < end:
        nxt = dt.date(cur.year + (cur.month // 12), cur.month % 12 + 1, 1)
        yield cur, min(nxt, end)
        cur = nxt


def fetch_month(start: dt.date, end: dt.date, min_mag: float) -> bytes:
    params = {
        "format": "geojson",
        "starttime": start.isoformat(),
        "endtime": end.isoformat(),
        "minmagnitude": min_mag,
        "orderby": "time",
    }
    for attempt in range(5):
        try:
            r = requests.get(USGS_FDSN, params=params, timeout=180)
            if r.status_code == 200:
                return r.content
            if r.status_code == 429:
                time.sleep(15 * (attempt + 1))
                continue
            r.raise_for_status()
        except requests.RequestException as exc:
            if attempt == 4:
                raise
            time.sleep(5 * (attempt + 1))
            print(f"  retry {attempt + 1}: {exc}", file=sys.stderr)
    raise RuntimeError("USGS fetch failed after retries")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/usgs")
    ap.add_argument("--years", type=int, default=5)
    ap.add_argument("--min-mag", type=float, default=2.5)
    ap.add_argument("--small", action="store_true",
                    help="6 months at M4.5+ (~13 MB) for quick pipeline testing")
    args = ap.parse_args()

    if args.small:
        years_back = 0.5
        min_mag = 4.5
    else:
        years_back = args.years
        min_mag = args.min_mag

    end = dt.date.today()
    start = end - dt.timedelta(days=int(365 * years_back))

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    chunks = list(month_ranges(start, end))
    print(f"USGS: {len(chunks)} monthly chunks, M{min_mag}+  ({start} to {end})")

    for s, e in tqdm(chunks, desc="USGS months"):
        target = out_dir / f"usgs_{s.isoformat()}.geojson"
        if target.exists() and target.stat().st_size > 200:
            continue
        data = fetch_month(s, e, min_mag)
        target.write_bytes(data)
        time.sleep(0.5)

    total = sum(p.stat().st_size for p in out_dir.glob("*.geojson"))
    print(f"USGS done: {total / 1e6:.1f} MB across {len(list(out_dir.glob('*.geojson')))} files")


if __name__ == "__main__":
    main()
