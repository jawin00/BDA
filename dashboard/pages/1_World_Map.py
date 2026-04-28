"""Page 1 — World map of historical disaster events."""

from __future__ import annotations

import folium
import pandas as pd
import streamlit as st
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium

from components.clients import mongo, safe_call

st.title("World Map of Historical Disasters")

cli = mongo()
coll = cli.disasters.events

with st.sidebar:
    st.subheader("Filters")
    types = ["all"] + sorted(safe_call(
        "distinct event_type",
        lambda: [t for t in coll.distinct("event_type") if t]) or [])
    pick_type = st.selectbox("event_type", types, index=0)
    min_sev = st.slider("min severity", 0.0, 1.0, 0.0, step=0.05)
    limit = st.slider("max markers", 100, 5000, 1500, step=100)

q: dict = {"lat": {"$ne": None}, "lon": {"$ne": None}}
if pick_type != "all":
    q["event_type"] = pick_type
if min_sev > 0:
    q["severity"] = {"$gte": min_sev}

cursor = coll.find(q, {
    "title": 1, "event_type": 1, "severity": 1,
    "lat": 1, "lon": 1, "country": 1, "ts": 1,
}).limit(limit)
rows = list(cursor)

st.write(f"**{len(rows):,}** events plotted (capped at {limit:,})")

if not rows:
    st.info("No events match those filters yet — run `./run_pipeline.sh` first.")
else:
    m = folium.Map(location=[20, 0], zoom_start=2, tiles="cartodbpositron")
    cluster = MarkerCluster().add_to(m)
    for r in rows:
        if r.get("lat") is None or r.get("lon") is None:
            continue
        popup = (
            f"<b>{r.get('event_type', '?')}</b><br>"
            f"sev={r.get('severity', 0):.2f}<br>"
            f"{r.get('title', '')[:120]}"
        )
        folium.CircleMarker(
            location=[r["lat"], r["lon"]],
            radius=4 + 6 * float(r.get("severity") or 0),
            popup=popup,
            color="#e63946",
            fill=True,
            fillOpacity=0.6,
        ).add_to(cluster)
    st_folium(
        m,
        key="historical_world_map",
        height=620,
        use_container_width=True,
        returned_objects=[],
    )
