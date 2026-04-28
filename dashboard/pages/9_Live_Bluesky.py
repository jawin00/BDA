"""Page 9 — Live Bluesky disaster stream.

Uses the public AT Protocol search endpoint
(https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts) which is
unauthenticated and free. Polls every 60 s for disaster keywords, upserts
matches into MongoDB.disasters.events with source='bluesky_live'.

This is the zero-credentials live tab — works out of the box, no signup,
no API key. Great fallback while Reddit access is pending.
"""

import datetime as dt
import time

import pandas as pd
import requests
import streamlit as st

from components.clients import mongo

st.title("Live Bluesky Disaster Stream")
st.caption(
    "Polls the public Bluesky AT Protocol search API every 60 s. "
    "No credentials required — uses public.api.bsky.app/xrpc/app.bsky.feed.searchPosts. "
    "Matches are upserted into MongoDB.disasters.events."
)

MONGO = mongo()
COLL = MONGO.disasters.events

KEYWORDS = ("earthquake", "flood", "hurricane", "wildfire", "tsunami",
            "volcano", "tornado", "cyclone", "landslide", "evacuation")

SEARCH_URL = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"

if "bsky_last_poll" not in st.session_state:
    st.session_state.bsky_last_poll = 0


def _fetch(query: str, limit: int = 25) -> list[dict]:
    try:
        r = requests.get(
            SEARCH_URL,
            params={"q": query, "limit": limit, "sort": "latest"},
            timeout=15,
        )
        if r.status_code != 200:
            return []
        return r.json().get("posts", [])
    except requests.RequestException:
        return []


def poll_once() -> int:
    inserted = 0
    seen_uris: set[str] = set()
    for kw in KEYWORDS:
        for post in _fetch(kw):
            uri = post.get("uri")
            if not uri or uri in seen_uris:
                continue
            seen_uris.add(uri)
            record = post.get("record", {})
            text = record.get("text", "") or ""
            if not text:
                continue
            try:
                created = dt.datetime.fromisoformat(
                    record.get("createdAt", "").replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                created = dt.datetime.utcnow()
            author = post.get("author", {}).get("handle", "unknown")
            doc = {
                "event_id": f"bsky_{uri.split('/')[-1]}",
                "source": "bluesky_live",
                "title": text[:140],
                "text": text,
                "ts": created,
                "country": None,
                "lat": None,
                "lon": None,
                "event_type": "live",
                "severity": 0.4,
                "cluster_id": None,
                "_author": author,
                "_uri": uri,
            }
            try:
                COLL.update_one(
                    {"event_id": doc["event_id"]},
                    {"$setOnInsert": doc},
                    upsert=True,
                )
                inserted += 1
            except Exception:
                pass
    return inserted


col1, col2 = st.columns([1, 1])
auto = col1.checkbox("auto-poll every 60 s", value=False)
if col2.button("poll now", type="primary"):
    n = poll_once()
    st.session_state.bsky_last_poll = time.time()
    st.success(f"Poll complete; {n} candidate posts upserted")

cutoff = dt.datetime.utcnow() - dt.timedelta(minutes=15)
recent = list(COLL.find(
    {"source": "bluesky_live", "ts": {"$gte": cutoff}},
    {"_id": 0, "title": 1, "ts": 1, "_author": 1, "_uri": 1},
).sort("ts", -1).limit(50))

st.write(f"Bluesky posts in last 15 min: **{len(recent)}**")
if recent:
    df = pd.DataFrame(recent)
    if "_uri" in df.columns:
        df["link"] = df["_uri"].apply(
            lambda u: f"https://bsky.app/profile/{u.split('/')[2]}/post/{u.split('/')[-1]}"
            if u else ""
        )
    st.dataframe(df, use_container_width=True)

if auto and time.time() - st.session_state.bsky_last_poll > 60:
    poll_once()
    st.session_state.bsky_last_poll = time.time()
    time.sleep(1)
    st.rerun()
