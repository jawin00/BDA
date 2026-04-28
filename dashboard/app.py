"""Disaster Intelligence Lakehouse — main dashboard entry.

Run:  streamlit run dashboard/app.py
"""

import streamlit as st

from components.clients import mongo, cassandra_count, thrift_conn, safe_call

st.set_page_config(
    page_title="Disaster Intelligence Lakehouse",
    page_icon=None,
    layout="wide",
)

st.title("Disaster Intelligence Lakehouse")
st.caption(
    "Multi-source disaster events ingested into HDFS, enriched with "
    "Spark + MapReduce + Scala, served from MongoDB + Cassandra + "
    "Spark SQL Thrift Server."
)

st.markdown("---")

st.subheader("Health checks")

col_a, col_b, col_c = st.columns(3)

with col_a:
    st.markdown("**MongoDB**")
    cli = safe_call("MongoDB", mongo)
    if cli is not None:
        n = safe_call("count events", lambda: cli.disasters.events.estimated_document_count())
        if n is not None:
            st.metric("events", f"{n:,}")

with col_b:
    st.markdown("**Cassandra**")
    n = safe_call("Cassandra count", cassandra_count)
    if n is not None:
        st.metric("events", f"{n:,}")

with col_c:
    st.markdown("**Spark Thrift**")
    conn = safe_call("Thrift JDBC", thrift_conn)
    if conn is not None:
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM events_enriched")
            n = cur.fetchone()[0]
            st.metric("events_enriched", f"{n:,}")
        except Exception as exc:
            st.warning(f"Thrift query failed: {exc}")

st.markdown("---")
st.markdown(
    "Use the page selector in the left sidebar to navigate. "
    "Pages are organized to mirror the syllabus tags — see "
    "`docs/syllabus_mapping.md` for the full mapping."
)

st.markdown(
    """
**Pages**
1. World Map — all events plotted on folium, filterable
2. Lead-Time Analysis — Scala-job results, the impact narrative
3. MongoDB Explorer — paginated documents
4. Cassandra Time-Series — per-region counts
5. Cluster Themes — K-means on TF-IDF
6. MapReduce Results — Java MR keyword frequency
7. Spark SQL Playground — write SQL, run via Thrift JDBC
8. Performance Lab — caching / broadcast / partition pruning timings
9. Live Bluesky — public AT Protocol search, no credentials needed
"""
)
