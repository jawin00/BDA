"""Page 5 — K-means cluster themes (TF-IDF + MLlib)."""

import pandas as pd
import streamlit as st

from components.clients import thrift_conn

st.title("Cluster Themes — K-means on TF-IDF")
st.caption("Pipeline: tokenize → stopwords → HashingTF → IDF → K-means(k=12).")

conn = thrift_conn()
cur = conn.cursor()

cur.execute("""
    SELECT cluster_id, token, n
      FROM cluster_terms
     ORDER BY cluster_id, n DESC
""")
cols = [d[0] for d in cur.description]
terms = pd.DataFrame(cur.fetchall(), columns=cols)

if terms.empty:
    st.info("Cluster job has not run — execute `./run_pipeline.sh`.")
    st.stop()

cluster_ids = sorted(terms["cluster_id"].unique())

st.subheader("Top terms per cluster")
top = (terms.groupby("cluster_id")
            .apply(lambda g: ", ".join(g.head(10)["token"].astype(str)))
            .reset_index(name="top_terms"))
st.dataframe(top, use_container_width=True)

st.subheader("Sample events per cluster")
pick = st.selectbox("cluster_id", cluster_ids, index=0)
cur.execute(f"""
    SELECT event_id, source, event_type, severity, title
      FROM events_clustered
     WHERE cluster_id = {int(pick)}
     LIMIT 50
""")
cols = [d[0] for d in cur.description]
samples = pd.DataFrame(cur.fetchall(), columns=cols)
st.dataframe(samples, use_container_width=True)
