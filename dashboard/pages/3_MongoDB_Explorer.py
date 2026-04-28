"""Page 3 — MongoDB document explorer."""

import pandas as pd
import streamlit as st

from components.clients import mongo

st.title("MongoDB Explorer — disasters.events")
st.caption("Random-access into the document store. All filters are pymongo queries.")

cli = mongo()
coll = cli.disasters.events

with st.sidebar:
    st.subheader("Filters")
    types = ["any"] + sorted([t for t in coll.distinct("event_type") if t])
    pick = st.selectbox("event_type", types)
    countries = ["any"] + sorted([c for c in coll.distinct("country") if c])[:200]
    pick_c = st.selectbox("country", countries)
    min_sev = st.slider("min severity", 0.0, 1.0, 0.0, step=0.05)
    page_size = st.slider("page size", 10, 200, 50, step=10)

q: dict = {}
if pick != "any":
    q["event_type"] = pick
if pick_c != "any":
    q["country"] = pick_c
if min_sev > 0:
    q["severity"] = {"$gte": min_sev}

total = coll.count_documents(q)
st.write(f"Matching documents: **{total:,}**")

if "page" not in st.session_state:
    st.session_state.page = 0

c1, c2, _ = st.columns([1, 1, 6])
if c1.button("◀ prev", disabled=st.session_state.page == 0):
    st.session_state.page = max(0, st.session_state.page - 1)
if c2.button("next ▶", disabled=(st.session_state.page + 1) * page_size >= total):
    st.session_state.page += 1

skip = st.session_state.page * page_size
docs = list(coll.find(q, {"_id": 0, "raw_json": 0}).sort("ts", -1).skip(skip).limit(page_size))
df = pd.DataFrame(docs)
st.dataframe(df, use_container_width=True)
