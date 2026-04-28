#!/usr/bin/env python3
"""NASA EONET v3 downloader.

Pulls all closed + open natural-event records from the last N years.
Single API call returns paginated JSON; small dataset (~50 MB).
"""

import argparse
import json
import os
import sys
from pathlib import Path

import requests

EONET_BASE = "https://eonet.gsfc.nasa.gov/api/v3/events"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/eonet")
    ap.add_argument("--days", type=int, default=365 * 5)
    ap.add_argument("--small", action="store_true")
    args = ap.parse_args()

    days = 180 if args.small else args.days

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"EONET: fetching last {days} days, all categories")
    params = {"days": days, "status": "all", "limit": 5000}
    r = requests.get(EONET_BASE, params=params, timeout=180)
    r.raise_for_status()
    data = r.json()

    target = out_dir / "eonet_events.json"
    target.write_bytes(json.dumps(data, indent=2).encode())

    n = len(data.get("events", []))
    size = target.stat().st_size / 1e6
    print(f"EONET done: {n} events, {size:.1f} MB")


if __name__ == "__main__":
    main()
