"""Page 6 — MapReduce keyword frequency results (loaded into Mongo)."""

import pandas as pd
import plotly.express as px
import streamlit as st

from components.clients import mongo

st.title("MapReduce — Keyword Frequency by Year × Country")
st.caption("Java/Hadoop MapReduce job (`pipeline/03_keyword_freq_mr/`). "
           "Results loaded into MongoDB.disasters.keyword_freq.")

cli = mongo()
coll = cli.disasters.keyword_freq

if coll.estimated_document_count() == 0:
    st.info("MR results not loaded — run `./run_pipeline.sh`.")
    st.stop()

years = sorted([y for y in coll.distinct("year") if y])
countries = sorted([c for c in coll.distinct("country") if c])

with st.sidebar:
    st.subheader("Filters")
    pick_year = st.selectbox("year", ["all"] + years, index=0)
    pick_country = st.selectbox("country", ["all"] + countries[:200], index=0)
    top_n = st.slider("top N terms", 5, 50, 20)

q: dict = {}
if pick_year != "all":
    q["year"] = pick_year
if pick_country != "all":
    q["country"] = pick_country

rows = list(coll.find(q, {"_id": 0}).sort("count", -1).limit(top_n))
df = pd.DataFrame(rows)

if df.empty:
    st.info("No rows for that filter.")
else:
    fig = px.bar(df.sort_values("count"), x="count", y="term", orientation="h",
                 color="country" if pick_country == "all" else None,
                 title=f"Top {top_n} disaster keywords")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, use_container_width=True)
