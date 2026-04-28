"""Page 9 — Live Bluesky disaster stream.

Uses the public AT Protocol search endpoint
(https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts) which is
unauthenticated and free. Polls every 60 s for disaster keywords, upserts
matches into MongoDB.disasters.events with source='bluesky_live'.

This is the zero-credentials live tab — works out of the box, no signup,
no API key. Great fallback while Reddit access is pending.
"""

import datetime as dt
import os
import time

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

from components.clients import mongo

load_dotenv()

st.title("Live Bluesky Disaster Stream")
st.caption(
    "Polls the public Bluesky AT Protocol search API every 60 s. "
    "No credentials required — uses public.api.bsky.app/xrpc/app.bsky.feed.searchPosts. "
    "Matches are upserted into MongoDB.disasters.events."
)

st.info(
    "Public Bluesky search may currently return HTTP 403. "
    "Optional authenticated fallback is supported via BLUESKY_HANDLE and "
    "BLUESKY_APP_PASSWORD in .env."
)

MONGO = mongo()
COLL = MONGO.disasters.events

KEYWORDS = ("earthquake", "flood", "hurricane", "wildfire", "tsunami",
            "volcano", "tornado", "cyclone", "landslide", "evacuation")

SEARCH_URL = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
AUTH_SEARCH_URL = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"
CREATE_SESSION_URL = "https://bsky.social/xrpc/com.atproto.server.createSession"
BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE", "").strip()
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD", "").strip()

if "bsky_last_poll" not in st.session_state:
    st.session_state.bsky_last_poll = 0
if "bsky_last_result" not in st.session_state:
    st.session_state.bsky_last_result = None


def _fetch(query: str, limit: int = 25) -> tuple[list[dict], str | None]:
    try:
        r = requests.get(
            SEARCH_URL,
            params={"q": query, "limit": limit, "sort": "latest"},
            headers={"User-Agent": "BDA-Streamlit/1.0"},
            timeout=(5, 20),
        )
        if r.status_code != 200:
            return [], f"{query}: HTTP {r.status_code}"
        return r.json().get("posts", []), None
    except requests.RequestException as exc:
        return [], f"{query}: {exc.__class__.__name__}: {exc}"


def _create_bsky_session() -> tuple[str | None, str | None]:
    if not BLUESKY_HANDLE or not BLUESKY_APP_PASSWORD:
        return None, None
    try:
        r = requests.post(
            CREATE_SESSION_URL,
            json={"identifier": BLUESKY_HANDLE, "password": BLUESKY_APP_PASSWORD},
            headers={"User-Agent": "BDA-Streamlit/1.0"},
            timeout=(5, 20),
        )
        if r.status_code != 200:
            return None, f"auth: HTTP {r.status_code} creating session for {BLUESKY_HANDLE}"
        token = (r.json() or {}).get("accessJwt")
        if not token:
            return None, "auth: missing accessJwt in session response"
        return token, None
    except requests.RequestException as exc:
        return None, f"auth: {exc.__class__.__name__}: {exc}"


def _fetch_authed(query: str, token: str, limit: int = 25) -> tuple[list[dict], str | None]:
    try:
        r = requests.get(
            AUTH_SEARCH_URL,
            params={"q": query, "limit": limit, "sort": "latest"},
            headers={
                "Authorization": f"Bearer {token}",
                "User-Agent": "BDA-Streamlit/1.0",
            },
            timeout=(5, 20),
        )
        if r.status_code != 200:
            return [], f"{query}: HTTP {r.status_code} (authenticated search)"
        return r.json().get("posts", []), None
    except requests.RequestException as exc:
        return [], f"{query}: {exc.__class__.__name__}: {exc}"


def poll_once() -> dict:
    fetched = 0
    inserted = 0
    errors: list[str] = []
    mode = "public"
    seen_uris: set[str] = set()

    token, auth_err = _create_bsky_session()
    if auth_err:
        errors.append(auth_err)
    if token:
        mode = "authenticated"

    for kw in KEYWORDS:
        posts, err = _fetch_authed(kw, token) if token else _fetch(kw)
        if err:
            errors.append(err)
            continue
        fetched += len(posts)
        for post in posts:
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
                result = COLL.update_one(
                    {"event_id": doc["event_id"]},
                    {"$setOnInsert": doc},
                    upsert=True,
                )
                if result.upserted_id is not None:
                    inserted += 1
            except Exception as exc:
                errors.append(f"mongo: {exc.__class__.__name__}: {exc}")
    return {
        "fetched": fetched,
        "inserted": inserted,
        "mode": mode,
        "errors": errors[:8],
    }


col1, col2 = st.columns([1, 1])
auto = col1.checkbox("auto-poll every 60 s", value=False)
if col2.button("poll now", type="primary"):
    result = poll_once()
    st.session_state.bsky_last_poll = time.time()
    st.session_state.bsky_last_result = result
    st.success(
        f"Poll complete ({result['mode']} mode); fetched {result['fetched']} posts, inserted {result['inserted']} new posts"
    )

cutoff = dt.datetime.utcnow() - dt.timedelta(minutes=15)
recent = list(COLL.find(
    {"source": "bluesky_live", "ts": {"$gte": cutoff}},
    {"_id": 0, "title": 1, "ts": 1, "_author": 1, "_uri": 1},
).sort("ts", -1).limit(50))

if st.session_state.bsky_last_result and st.session_state.bsky_last_result.get("errors"):
    st.warning("Bluesky poll issues:\n\n- " + "\n- ".join(st.session_state.bsky_last_result["errors"]))
    if (
        not BLUESKY_HANDLE
        and any("HTTP 403" in msg for msg in st.session_state.bsky_last_result["errors"])
    ):
        st.info(
            "The public Bluesky search endpoint is refusing anonymous requests right now. "
            "To enable authenticated fallback, add BLUESKY_HANDLE and BLUESKY_APP_PASSWORD to .env."
        )

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
    st.session_state.bsky_last_result = poll_once()
    st.session_state.bsky_last_poll = time.time()
    time.sleep(1)
    st.rerun()
