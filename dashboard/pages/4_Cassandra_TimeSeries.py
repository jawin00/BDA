"""Page 4 — Cassandra time-series, partitioned by (country, date_bucket)."""

import pandas as pd
import plotly.express as px
import streamlit as st

from components.clients import cassandra_countries, cassandra_events, safe_call

st.title("Cassandra — events_by_region_date")
st.caption(
    "Partition key: (country, date_bucket=YYYY-MM). Cluster key: event_time DESC. "
    "Demonstrates why Cassandra is the right pick for time-series per region."
)

countries = safe_call("load Cassandra countries", cassandra_countries) or []

with st.sidebar:
    st.subheader("Filters")
    if not countries:
        st.warning("No countries yet — run the pipeline.")
        st.stop()
    pick = st.selectbox("country", countries, index=0)
    months_back = st.slider("months window", 1, 60, 24)

today = pd.Timestamp.utcnow().normalize()
buckets = [(today - pd.DateOffset(months=i)).strftime("%Y-%m") for i in range(months_back)]

rows = safe_call("load Cassandra rows", cassandra_events, pick, tuple(buckets)) or []
df = pd.DataFrame(rows)
st.write(f"Rows for {pick} (last {months_back} months): **{len(df):,}**")

if df.empty:
    st.info("No data for that selection.")
else:
    df["month"] = pd.to_datetime(df["ts"]).dt.strftime("%Y-%m")
    counts = df.groupby(["month", "event_type"]).size().reset_index(name="n")
    fig = px.bar(counts.sort_values("month"), x="month", y="n", color="event_type",
                 barmode="stack", title=f"{pick} — events per month by type")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df.sort_values("ts", ascending=False).head(200),
                 use_container_width=True)
