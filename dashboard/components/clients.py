"""Cached clients and lightweight backend adapters for the dashboard."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import streamlit as st


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
THRIFT_HOST = os.getenv("THRIFT_HOST", "localhost")
THRIFT_PORT = int(os.getenv("THRIFT_PORT", "10000"))
_CASSANDRA_WORKER = Path(__file__).with_name("cassandra_worker.py")


@st.cache_resource(show_spinner=False)
def mongo():
    from pymongo import MongoClient

    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)


@st.cache_resource(show_spinner=False)
def thrift_conn():
    from pyhive import hive

    return hive.Connection(host=THRIFT_HOST, port=THRIFT_PORT, auth="NONE")


def _run_cassandra_worker(payload: dict, timeout: int = 20) -> dict:
    env = os.environ.copy()
    env["CASSANDRA_HOST"] = CASSANDRA_HOST
    proc = subprocess.run(
        [sys.executable, str(_CASSANDRA_WORKER)],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        timeout=timeout,
        env=env,
        check=False,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip() or f"exit code {proc.returncode}"
        raise RuntimeError(detail)
    return json.loads(proc.stdout)


@st.cache_data(ttl=60, show_spinner=False)
def cassandra_count() -> int:
    data = _run_cassandra_worker({"op": "count_events"}, timeout=15)
    return int(data["count"])


@st.cache_data(ttl=600, show_spinner=False)
def cassandra_countries() -> list[str]:
    data = _run_cassandra_worker({"op": "list_countries"}, timeout=20)
    return list(data["countries"])


@st.cache_data(ttl=300, show_spinner=False)
def cassandra_events(country: str, buckets: tuple[str, ...]) -> list[dict]:
    data = _run_cassandra_worker(
        {"op": "events_for_country_buckets", "country": country, "buckets": list(buckets)},
        timeout=40,
    )
    return list(data["rows"])


def safe_call(label: str, fn, *args, **kwargs):
    """Run a backend call and surface errors as a Streamlit warning, not a crash."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        st.warning(f"{label}: backend unavailable ({exc.__class__.__name__}: {exc})")
        return None
