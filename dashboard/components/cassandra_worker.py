"""Isolated Cassandra worker for Streamlit.

Runs in a plain Python subprocess so the gevent/Cassandra integration stays
out of Streamlit's script thread on Python 3.12.
"""

from __future__ import annotations

import json
import os
import sys
import warnings

import gevent.monkey

gevent.monkey.patch_all()

warnings.filterwarnings("ignore", category=DeprecationWarning)

from cassandra.cluster import Cluster
from cassandra.io.geventreactor import GeventConnection


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")


def _connect():
    cluster = Cluster([CASSANDRA_HOST], connection_class=GeventConnection, connect_timeout=15)
    return cluster, cluster.connect("disasters")


def _count_events(sess) -> dict:
    row = sess.execute("SELECT COUNT(*) FROM events_by_region_date").one()
    return {"count": int(row[0] or 0)}


def _list_countries(sess) -> dict:
    rows = sess.execute(
        "SELECT DISTINCT country, date_bucket FROM events_by_region_date"
    )
    countries = []
    seen = set()
    for row in rows:
        country = getattr(row, "country", None)
        if country and country not in seen:
            seen.add(country)
            countries.append(country)
    return {"countries": countries[:200]}


def _events_for_country_buckets(sess, country: str, buckets: list[str]) -> dict:
    stmt = sess.prepare(
        """
        SELECT event_time, event_type, severity, title
          FROM events_by_region_date
         WHERE country = ? AND date_bucket = ?
        """
    )
    rows = []
    for bucket in buckets:
        for row in sess.execute(stmt, (country, bucket)):
            rows.append(
                {
                    "ts": row.event_time.isoformat() if row.event_time else None,
                    "event_type": row.event_type,
                    "severity": float(row.severity) if row.severity is not None else None,
                    "title": row.title,
                }
            )
    return {"rows": rows}


def main() -> int:
    request = json.load(sys.stdin)
    cluster, sess = _connect()
    try:
        op = request.get("op")
        if op == "count_events":
            result = _count_events(sess)
        elif op == "list_countries":
            result = _list_countries(sess)
        elif op == "events_for_country_buckets":
            result = _events_for_country_buckets(
                sess,
                str(request["country"]),
                [str(bucket) for bucket in request.get("buckets", [])],
            )
        else:
            raise ValueError(f"unknown op: {op}")
    finally:
        cluster.shutdown()

    json.dump(result, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
