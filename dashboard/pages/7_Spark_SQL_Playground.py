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

QUERIES = {
    "Default (Severity by Event)": """SELECT event_type,
       COUNT(*)        AS n,
       AVG(severity)   AS avg_sev
  FROM events_enriched
 GROUP BY event_type
 ORDER BY n DESC""",
    "Global Heatmap (Top Countries)": """SELECT geo_country, 
       COUNT(*) AS total_events
  FROM events_enriched
 WHERE geo_country IS NOT NULL
 GROUP BY geo_country
 ORDER BY total_events DESC
 LIMIT 15""",
    "Pipeline Ingestion (Data Sources)": """SELECT source, 
       COUNT(*) AS event_count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM events_enriched), 2) AS percentage
  FROM events_enriched
 GROUP BY source
 ORDER BY event_count DESC""",
    "High-Confidence Unstructured Events": """SELECT source, 
       event_type, 
       title, 
       ROUND(classify_conf, 3) AS confidence, 
       geo_country
  FROM events_enriched
 WHERE classify_conf > 0.90 
   AND source != 'usgs'
 ORDER BY classify_conf DESC
 LIMIT 10""",
    "Machine Learning Cluster Themes": """SELECT e.cluster_id, 
       COUNT(e.event_id) AS event_count,
       t.top_terms
  FROM events_clustered e
  JOIN (
       SELECT cluster_id, concat_ws(', ', collect_list(token)) AS top_terms
         FROM (
              SELECT cluster_id, token 
                FROM cluster_terms 
               WHERE rank <= 5
               ORDER BY cluster_id, rank
         )
        GROUP BY cluster_id
  ) t ON e.cluster_id = t.cluster_id
 GROUP BY e.cluster_id, t.top_terms
 ORDER BY event_count DESC"""
}

selected_template = st.selectbox("Choose a pre-built query template:", list(QUERIES.keys()))
q = st.text_area("SQL", value=QUERIES[selected_template], height=200)

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
