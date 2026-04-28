#!/usr/bin/env python3
"""GDACS historical alerts downloader.

GDACS exposes a search API that returns alerts as JSON. We pull the last N
years of EQ/TC/FL/VO/DR alerts. Output: one JSON per year-event-type chunk.
"""

import argparse
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path

import requests

GDACS_SEARCH = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
EVENT_TYPES = ["EQ", "TC", "FL", "VO", "DR", "WF"]


def fetch_year(event_type: str, from_date: dt.date, to_date: dt.date) -> dict:
    params = {
        "eventlist": event_type,
        "fromDate": from_date.isoformat(),
        "toDate": to_date.isoformat(),
        "alertlevel": "Green;Orange;Red",
    }
    for attempt in range(5):
        try:
            r = requests.get(GDACS_SEARCH, params=params, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 503):
                time.sleep(15 * (attempt + 1))
                continue
            r.raise_for_status()
        except requests.RequestException as exc:
            if attempt == 4:
                raise
            print(f"  retry {attempt + 1}: {exc}", file=sys.stderr)
            time.sleep(5 * (attempt + 1))
    raise RuntimeError("GDACS fetch failed after retries")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/gdacs")
    ap.add_argument("--years", type=int, default=5)
    ap.add_argument("--small", action="store_true")
    args = ap.parse_args()

    years = 1 if args.small else args.years

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    end = dt.date.today()
    start_year = end.year - years

    print(f"GDACS: {years} years × {len(EVENT_TYPES)} event types")

    for year in range(start_year, end.year + 1):
        for et in EVENT_TYPES:
            target = out_dir / f"gdacs_{year}_{et}.json"
            if target.exists() and target.stat().st_size > 200:
                continue
            from_date = dt.date(year, 1, 1)
            to_date = dt.date(year, 12, 31) if year < end.year else end
            try:
                data = fetch_year(et, from_date, to_date)
                target.write_bytes(json.dumps(data).encode())
                n = len(data.get("features", [])) if isinstance(data, dict) else 0
                print(f"  {year}/{et}: {n} alerts")
            except Exception as exc:
                print(f"  {year}/{et}: SKIP ({exc})", file=sys.stderr)
            time.sleep(0.5)

    total = sum(p.stat().st_size for p in out_dir.glob("*.json"))
    print(f"GDACS done: {total / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
