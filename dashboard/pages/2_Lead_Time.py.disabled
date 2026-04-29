"""Page 2 — Lead-time analysis (the headline impact narrative)."""

import pandas as pd
import plotly.express as px
import streamlit as st

from components.clients import thrift_conn

st.title("Lead-Time: Social vs Official Alerts")
st.caption(
    "For each matched (official, social) event pair within ±48h and ≤250 km, "
    "Δt = social_ts − official_ts. Negative Δt means social mentions came first."
)

conn = thrift_conn()
cur = conn.cursor()

cur.execute("SELECT * FROM lead_time_summary ORDER BY n_pairs DESC")
cols = [d[0] for d in cur.description]
summary = pd.DataFrame(cur.fetchall(), columns=cols)

if summary.empty:
    st.info("Lead-time job has not produced output yet — run `./run_pipeline.sh`.")
    st.stop()

st.subheader("Summary (per event_type)")
st.dataframe(summary, use_container_width=True)

st.subheader("Median Δt (minutes) by event type")
fig = px.bar(
    summary, x="event_type", y="median_delta_minutes",
    color="median_delta_minutes",
    color_continuous_scale="RdBu_r",
    labels={"median_delta_minutes": "median Δt (min)"},
    title="Negative = social precedes official, Positive = official precedes social",
)
fig.add_hline(y=0, line_dash="dash", line_color="black")
st.plotly_chart(fig, use_container_width=True)

st.subheader("Distribution sample (per event type)")
cur.execute("""
    SELECT event_type, delta_seconds / 60.0 AS delta_minutes
      FROM lead_time_matches
     WHERE event_type IN (
            SELECT event_type FROM lead_time_summary
             ORDER BY n_pairs DESC LIMIT 4)
     LIMIT 5000
""")
cols = [d[0] for d in cur.description]
samples = pd.DataFrame(cur.fetchall(), columns=cols)
if not samples.empty:
    fig2 = px.histogram(
        samples, x="delta_minutes", color="event_type",
        nbins=60, marginal="box", opacity=0.7,
    )
    fig2.add_vline(x=0, line_dash="dash", line_color="black")
    st.plotly_chart(fig2, use_container_width=True)
