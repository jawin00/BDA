"""Page 7 — Spark SQL playground (queries dispatched over JDBC to Thrift)."""

import pandas as pd
import streamlit as st

from components.clients import thrift_conn

st.title("Spark SQL Playground (JDBC → Thrift Server)")
st.caption(
    "Type any Spark SQL query. It runs on the Thrift Server at "
    "jdbc:hive2://spark-thrift:10000. "
    "Tables: events_enriched, events_clustered, lead_time_matches, "
    "lead_time_summary, cluster_terms."
)

DEFAULT = """SELECT event_type,
       COUNT(*)        AS n,
       AVG(severity)   AS avg_sev
  FROM events_enriched
 GROUP BY event_type
 ORDER BY n DESC"""

q = st.text_area("SQL", value=DEFAULT, height=200)

if st.button("Run", type="primary"):
    conn = thrift_conn()
    cur = conn.cursor()
    try:
        cur.execute(q)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        df = pd.DataFrame(rows, columns=cols)
        st.success(f"{len(df):,} rows")
        st.dataframe(df, use_container_width=True)
    except Exception as exc:
        st.error(f"Query failed: {exc}")
