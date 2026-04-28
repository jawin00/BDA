"""Page 8 — Spark SQL performance experiments.

Runs the same query under three conditions and shows wall-clock timings:
  1. Cold (no cache, no broadcast hint)
  2. With CACHE TABLE
  3. With broadcast join hint

Demonstrates the Spark SQL Performance subtag end-to-end.
"""

import socket
import time

import pandas as pd
import streamlit as st

from components.clients import thrift_conn

st.title("Spark SQL — Performance Lab")
st.caption("Same query, three conditions. Times reported are end-to-end JDBC RTT.")

QUERY_BASE = """
SELECT e.event_type,
       e.country,
       COUNT(*) AS n
  FROM events_enriched e
  JOIN cluster_terms  c
    ON c.cluster_id = 0
 GROUP BY e.event_type, e.country
"""


def detect_spark_ui_url() -> str | None:
    for port in range(4040, 4051):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return f"http://localhost:{port}"
        except OSError:
            continue
    return None

if st.button("Run benchmark", type="primary"):
    conn = thrift_conn()
    cur = conn.cursor()

    results = []

    # Make sure no leftover cache state.
    try:
        cur.execute("UNCACHE TABLE IF EXISTS events_enriched")
        cur.execute("UNCACHE TABLE IF EXISTS cluster_terms")
    except Exception:
        pass

    # 1. Cold
    t0 = time.perf_counter()
    cur.execute(QUERY_BASE)
    cur.fetchall()
    t_cold = time.perf_counter() - t0
    results.append(("cold (no cache, no hint)", t_cold))

    # 2. Cached
    cur.execute("CACHE TABLE events_enriched")
    cur.execute("CACHE TABLE cluster_terms")
    t0 = time.perf_counter()
    cur.execute(QUERY_BASE)
    cur.fetchall()
    t_cached = time.perf_counter() - t0
    results.append(("cached", t_cached))

    # 3. Broadcast hint
    q3 = QUERY_BASE.replace("SELECT e.event_type,",
                            "SELECT /*+ BROADCAST(c) */ e.event_type,")
    t0 = time.perf_counter()
    cur.execute(q3)
    cur.fetchall()
    t_bcast = time.perf_counter() - t0
    results.append(("cached + broadcast hint", t_bcast))

    cur.execute("UNCACHE TABLE events_enriched")
    cur.execute("UNCACHE TABLE cluster_terms")

    df = pd.DataFrame(results, columns=["condition", "seconds"])
    df["speedup_vs_cold"] = (df["seconds"].iloc[0] / df["seconds"]).round(2)
    st.dataframe(df, use_container_width=True)
    st.bar_chart(df.set_index("condition")["seconds"])

    ui_url = detect_spark_ui_url()
    if ui_url:
        st.markdown(f"Open the [Spark UI]({ui_url}) for the physical plans behind each run.")
    else:
        st.info("Spark UI was not detected on localhost:4040-4050.")
