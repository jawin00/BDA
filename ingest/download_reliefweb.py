#!/usr/bin/env python3
"""ReliefWeb API v2 downloader.

Pulls disaster reports for the last 2 years using their /reports endpoint.
Paginates 1000 per request via offset. Output: one JSON per page.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

RW_BASE = "https://api.reliefweb.int/v2/reports"


def fetch_page(appname: str, offset: int, limit: int, days: int) -> dict:
    payload = {
        "limit": limit,
        "offset": offset,
        "fields": {
            "include": [
                "title", "body-html", "date", "country", "primary_country",
                "disaster", "disaster_type", "source", "url", "language",
            ]
        },
        "filter": {
            "operator": "AND",
            "conditions": [
                {"field": "language.code", "value": "en"},
                {"field": "date.created",
                 "value": {"from": _iso_days_ago(days)}},
                {"field": "disaster_type"},
            ],
        },
        "sort": ["date.created:desc"],
    }
    for attempt in range(5):
        try:
            r = requests.post(RW_BASE, params={"appname": appname}, json=payload, timeout=120)
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
    raise RuntimeError("ReliefWeb fetch failed after retries")


def _iso_days_ago(days: int) -> str:
    import datetime as dt
    return (dt.datetime.utcnow() - dt.timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/reliefweb")
    ap.add_argument("--appname", default=os.getenv("RELIEFWEB_APPNAME", "disaster-intel-bda"))
    ap.add_argument("--days", type=int, default=730)
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--max-pages", type=int, default=20)
    ap.add_argument("--small", action="store_true")
    args = ap.parse_args()

    if args.small:
        args.days = 90
        args.max_pages = 3

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"ReliefWeb: appname={args.appname}, last {args.days}d, up to {args.max_pages} pages")

    total = 0
    for page in range(args.max_pages):
        offset = page * args.limit
        target = out_dir / f"reliefweb_p{page:03d}.json"
        if target.exists() and target.stat().st_size > 1000:
            continue
        data = fetch_page(args.appname, offset, args.limit, args.days)
        target.write_bytes(json.dumps(data).encode())
        n = len(data.get("data", []))
        total += n
        print(f"  page {page}: {n} reports")
        if n < args.limit:
            break
        time.sleep(1.0)

    print(f"ReliefWeb done: ~{total} reports across {page + 1} pages")


if __name__ == "__main__":
    main()
