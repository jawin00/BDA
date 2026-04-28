"""Page 10 - rich cross-source analytics over the disaster lakehouse."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from components.clients import thrift_conn


QUALITY_COLS = [
    "has_raw_coords",
    "has_geo_coords",
    "has_country",
    "has_geo_country",
    "has_text",
    "has_classify_conf",
]

QUALITY_LABELS = {
    "has_raw_coords": "raw coordinates",
    "has_geo_coords": "geoparse coordinates",
    "has_country": "country present",
    "has_geo_country": "geoparse country",
    "has_text": "non-empty text",
    "has_classify_conf": "classification confidence",
}

WEEKDAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
SEVERITY_BANDS = ["very low", "low", "medium", "high", "critical"]


st.title("Analytics Hub")
st.caption(
    "Cross-source analytics over the enriched lakehouse: volume, timing, "
    "severity, geography, data quality, and cluster behavior."
)


@st.cache_data(ttl=300, show_spinner=False)
def load_events() -> pd.DataFrame:
    conn = thrift_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT source,
               event_type,
               ts,
               severity,
               CASE
                   WHEN country IS NOT NULL AND TRIM(country) <> '' THEN country
                   WHEN geo_country IS NOT NULL AND TRIM(geo_country) <> '' THEN geo_country
                   ELSE 'UNK'
               END AS country_resolved,
               CASE WHEN lat IS NOT NULL AND lon IS NOT NULL THEN 1 ELSE 0 END AS has_raw_coords,
               CASE WHEN geo_lat IS NOT NULL AND geo_lon IS NOT NULL THEN 1 ELSE 0 END AS has_geo_coords,
               CASE WHEN country IS NOT NULL AND TRIM(country) <> '' THEN 1 ELSE 0 END AS has_country,
               CASE WHEN geo_country IS NOT NULL AND TRIM(geo_country) <> '' THEN 1 ELSE 0 END AS has_geo_country,
               CASE WHEN text IS NOT NULL AND LENGTH(TRIM(text)) > 0 THEN 1 ELSE 0 END AS has_text,
               CASE WHEN classify_conf IS NOT NULL THEN 1 ELSE 0 END AS has_classify_conf
          FROM events_enriched
        """
    )
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df

    df["ts"] = pd.to_datetime(df["ts"], format="ISO8601", errors="coerce")
    df = df.dropna(subset=["ts"]).copy()
    df["severity"] = pd.to_numeric(df["severity"], errors="coerce").fillna(0.0)
    df["source"] = df["source"].fillna("unknown")
    df["event_type"] = df["event_type"].fillna("unknown")
    df["country_resolved"] = df["country_resolved"].fillna("UNK")
    for col in QUALITY_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_clustered() -> pd.DataFrame:
    conn = thrift_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT cluster_id, event_type, severity
          FROM events_clustered
        """
    )
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df
    df["severity"] = pd.to_numeric(df["severity"], errors="coerce").fillna(0.0)
    df["event_type"] = df["event_type"].fillna("unknown")
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_cluster_terms() -> pd.DataFrame:
    conn = thrift_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT cluster_id, token, n
          FROM cluster_terms
         ORDER BY cluster_id, n DESC
        """
    )
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


try:
    events = load_events()
except Exception as exc:
    st.error(f"Could not load analytics base table: {exc}")
    st.stop()

if events.empty:
    st.info("Analytics are not available yet - run the pipeline first.")
    st.stop()

try:
    clustered = load_clustered()
    cluster_terms = load_cluster_terms()
except Exception:
    clustered = pd.DataFrame()
    cluster_terms = pd.DataFrame()

max_ts = events["ts"].max().normalize()
min_ts = events["ts"].min().normalize()
span_days = max(30, int((max_ts - min_ts).days) + 1)

with st.sidebar:
    st.subheader("Analytics Filters")
    days_back = st.slider("window (days)", 30, span_days, min(180, span_days), step=30)
    sources = sorted(events["source"].dropna().unique().tolist())
    selected_sources = st.multiselect("sources", sources, default=sources)
    top_n_countries = st.slider("top countries", 5, 25, 12)

window_start = max_ts - pd.Timedelta(days=days_back - 1)
filtered = events[events["ts"] >= window_start].copy()
if selected_sources:
    filtered = filtered[filtered["source"].isin(selected_sources)].copy()

if filtered.empty:
    st.warning("No analytics rows match the current filters.")
    st.stop()

filtered["day"] = filtered["ts"].dt.floor("D")
filtered["month"] = filtered["ts"].dt.strftime("%Y-%m")
filtered["weekday"] = pd.Categorical(
    filtered["ts"].dt.day_name().str[:3],
    categories=WEEKDAY_ORDER,
    ordered=True,
)
filtered["hour"] = filtered["ts"].dt.hour
filtered["has_any_geo"] = (
    (filtered["has_raw_coords"] == 1) | (filtered["has_geo_coords"] == 1)
).astype(int)
filtered["completeness_score"] = filtered[QUALITY_COLS].mean(axis=1)
filtered["severity_band"] = pd.cut(
    filtered["severity"].clip(lower=0.0, upper=1.0),
    bins=[-0.01, 0.20, 0.40, 0.60, 0.80, 1.01],
    labels=SEVERITY_BANDS,
)

known_country = filtered[filtered["country_resolved"] != "UNK"].copy()
daily_total = (
    filtered.groupby("day")
    .size()
    .reset_index(name="n")
    .sort_values("day")
)
daily_total["rolling_7d"] = daily_total["n"].rolling(7, min_periods=1).mean()
daily_source = (
    filtered.groupby(["day", "source"])
    .size()
    .reset_index(name="n")
    .sort_values(["day", "source"])
)
daily_source["share_pct"] = (
    daily_source["n"]
    / daily_source.groupby("day")["n"].transform("sum")
    * 100.0
)

top_types = filtered["event_type"].value_counts().head(8).index.tolist()
top_type_trend = (
    filtered[filtered["event_type"].isin(top_types)]
    .groupby(["day", "event_type"])
    .size()
    .reset_index(name="n")
    .sort_values("day")
)

country_counts = (
    known_country.groupby("country_resolved")
    .size()
    .reset_index(name="n")
    .sort_values("n", ascending=False)
)
top_countries = country_counts.head(top_n_countries)["country_resolved"].tolist()

tabs = st.tabs(["Overview", "Temporal", "Severity", "Geography", "Quality", "Clusters"])

with tabs[0]:
    avg_daily = len(filtered) / max(1, daily_total["day"].nunique())
    peak_row = daily_total.sort_values("n", ascending=False).iloc[0]
    top_country_share = 0.0
    if not country_counts.empty:
        top_country_share = 100.0 * country_counts.head(top_n_countries)["n"].sum() / len(filtered)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("events", f"{len(filtered):,}")
    m2.metric("avg per day", f"{avg_daily:.1f}")
    m3.metric("peak day volume", f"{int(peak_row['n']):,}")
    m4.metric("median severity", f"{filtered['severity'].median():.2f}")

    m5, m6, m7, m8 = st.columns(4)
    m5.metric("sources", f"{filtered['source'].nunique():,}")
    m6.metric(
        "countries",
        f"{known_country['country_resolved'].nunique():,}",
    )
    m7.metric("event types", f"{filtered['event_type'].nunique():,}")
    m8.metric("top-country share", f"{top_country_share:.1f}%")

    c1, c2 = st.columns(2)
    with c1:
        fig_daily = px.line(
            daily_source,
            x="day",
            y="n",
            color="source",
            markers=True,
            title="Daily event volume by source",
        )
        st.plotly_chart(fig_daily, use_container_width=True)
    with c2:
        fig_roll = px.line(
            daily_total,
            x="day",
            y=["n", "rolling_7d"],
            title="Daily volume and 7-day rolling average",
            labels={"value": "events", "variable": "series"},
        )
        st.plotly_chart(fig_roll, use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        fig_share = px.area(
            daily_source,
            x="day",
            y="share_pct",
            color="source",
            title="Source share over time",
            labels={"share_pct": "share (%)"},
        )
        st.plotly_chart(fig_share, use_container_width=True)
    with c4:
        mix_pivot = (
            filtered.groupby(["source", "event_type"])
            .size()
            .reset_index(name="n")
            .pivot(index="source", columns="event_type", values="n")
            .fillna(0)
        )
        fig_mix = px.imshow(
            mix_pivot,
            text_auto=True,
            aspect="auto",
            color_continuous_scale="Teal",
            title="Source x event-type volume",
        )
        st.plotly_chart(fig_mix, use_container_width=True)

with tabs[1]:
    c1, c2 = st.columns(2)
    with c1:
        weekday_hour = (
            filtered.groupby(["weekday", "hour"], observed=False)
            .size()
            .unstack(fill_value=0)
            .reindex(WEEKDAY_ORDER)
            .fillna(0)
        )
        fig_weekday = px.imshow(
            weekday_hour,
            text_auto=True,
            aspect="auto",
            color_continuous_scale="Cividis",
            title="Weekday x hour activity heatmap",
        )
        st.plotly_chart(fig_weekday, use_container_width=True)
    with c2:
        source_tempo = (
            daily_source.groupby("source")
            .agg(
                avg_daily=("n", "mean"),
                peak_day=("n", "max"),
                active_days=("day", "nunique"),
            )
            .reset_index()
            .sort_values("avg_daily", ascending=False)
        )
        source_tempo["peak_to_avg"] = (source_tempo["peak_day"] / source_tempo["avg_daily"]).round(2)
        fig_tempo = px.bar(
            source_tempo,
            x="source",
            y="avg_daily",
            color="peak_to_avg",
            hover_data=["peak_day", "active_days"],
            title="Source tempo: average daily load vs burstiness",
        )
        st.plotly_chart(fig_tempo, use_container_width=True)

    fig_type_trend = px.line(
        top_type_trend,
        x="day",
        y="n",
        color="event_type",
        markers=True,
        title="Top event types over time",
    )
    st.plotly_chart(fig_type_trend, use_container_width=True)

with tabs[2]:
    c1, c2 = st.columns(2)
    with c1:
        sev_box = filtered[filtered["event_type"].isin(top_types)].copy()
        fig_box = px.box(
            sev_box,
            x="event_type",
            y="severity",
            color="event_type",
            points="outliers",
            title="Severity distribution for top event types",
        )
        st.plotly_chart(fig_box, use_container_width=True)
    with c2:
        fig_hist = px.histogram(
            filtered,
            x="severity",
            color="source",
            nbins=30,
            barmode="overlay",
            opacity=0.7,
            title="Severity histogram by source",
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        severity_mix = (
            filtered.groupby(["source", "severity_band"], observed=False)
            .size()
            .reset_index(name="n")
        )
        severity_mix["pct"] = (
            severity_mix["n"]
            / severity_mix.groupby("source")["n"].transform("sum")
            * 100.0
        )
        fig_bands = px.bar(
            severity_mix,
            x="source",
            y="pct",
            color="severity_band",
            category_orders={"severity_band": SEVERITY_BANDS},
            title="Severity-band mix by source",
            labels={"pct": "share (%)", "severity_band": "severity band"},
        )
        st.plotly_chart(fig_bands, use_container_width=True)
    with c4:
        avg_sev_heat = (
            filtered[filtered["event_type"].isin(top_types)]
            .groupby(["source", "event_type"])["severity"]
            .mean()
            .reset_index()
            .pivot(index="source", columns="event_type", values="severity")
            .fillna(0)
        )
        fig_sev_heat = px.imshow(
            avg_sev_heat.round(2),
            text_auto=".2f",
            aspect="auto",
            color_continuous_scale="OrRd",
            title="Average severity by source and event type",
        )
        st.plotly_chart(fig_sev_heat, use_container_width=True)

    if known_country.empty:
        st.info("No resolved country data is available for risk analytics.")
    else:
        country_risk = (
            known_country.groupby("country_resolved")
            .agg(
                events=("country_resolved", "size"),
                avg_severity=("severity", "mean"),
                max_severity=("severity", "max"),
                geo_coverage=("has_any_geo", "mean"),
            )
            .reset_index()
            .sort_values("events", ascending=False)
            .head(top_n_countries)
        )
        country_risk["geo_coverage"] = (country_risk["geo_coverage"] * 100.0).round(1)
        fig_risk = px.scatter(
            country_risk,
            x="events",
            y="avg_severity",
            size="max_severity",
            color="geo_coverage",
            hover_name="country_resolved",
            title="Top countries: event volume vs average severity",
            labels={"geo_coverage": "geo coverage (%)"},
        )
        st.plotly_chart(fig_risk, use_container_width=True)
        st.dataframe(country_risk.round({"avg_severity": 3, "max_severity": 3}), use_container_width=True)

with tabs[3]:
    if known_country.empty:
        st.info("No resolved country data is available for geography analytics.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            fig_country = px.bar(
                country_counts.head(top_n_countries).sort_values("n"),
                x="n",
                y="country_resolved",
                orientation="h",
                title="Top countries by event count",
            )
            st.plotly_chart(fig_country, use_container_width=True)
        with c2:
            month_country = (
                known_country[known_country["country_resolved"].isin(top_countries)]
                .groupby(["month", "country_resolved"])
                .size()
                .reset_index(name="n")
                .sort_values("month")
            )
            fig_month_country = px.line(
                month_country,
                x="month",
                y="n",
                color="country_resolved",
                markers=True,
                title="Monthly trend for top countries",
            )
            st.plotly_chart(fig_month_country, use_container_width=True)

        c3, c4 = st.columns(2)
        with c3:
            source_country = (
                known_country[known_country["country_resolved"].isin(top_countries)]
                .groupby(["country_resolved", "source"])
                .size()
                .reset_index(name="n")
                .pivot(index="country_resolved", columns="source", values="n")
                .fillna(0)
            )
            fig_source_country = px.imshow(
                source_country,
                text_auto=True,
                aspect="auto",
                color_continuous_scale="PuBuGn",
                title="Country x source contribution",
            )
            st.plotly_chart(fig_source_country, use_container_width=True)
        with c4:
            country_latest = (
                known_country.groupby("country_resolved")
                .agg(
                    events=("country_resolved", "size"),
                    avg_severity=("severity", "mean"),
                    latest_ts=("ts", "max"),
                )
                .reset_index()
                .sort_values("events", ascending=False)
                .head(top_n_countries)
            )
            country_latest["latest_ts"] = country_latest["latest_ts"].dt.strftime("%Y-%m-%d %H:%M")
            st.dataframe(
                country_latest.round({"avg_severity": 3}),
                use_container_width=True,
            )

with tabs[4]:
    quality_summary = pd.DataFrame(
        [
            (QUALITY_LABELS["has_raw_coords"], filtered["has_raw_coords"].mean()),
            (QUALITY_LABELS["has_geo_coords"], filtered["has_geo_coords"].mean()),
            (QUALITY_LABELS["has_country"], filtered["has_country"].mean()),
            (QUALITY_LABELS["has_geo_country"], filtered["has_geo_country"].mean()),
            (QUALITY_LABELS["has_text"], filtered["has_text"].mean()),
            (QUALITY_LABELS["has_classify_conf"], filtered["has_classify_conf"].mean()),
            ("any geo coverage", filtered["has_any_geo"].mean()),
            ("overall completeness", filtered["completeness_score"].mean()),
        ],
        columns=["metric", "coverage"],
    )
    quality_summary["coverage_pct"] = (quality_summary["coverage"] * 100).round(1)

    c1, c2 = st.columns(2)
    with c1:
        fig_quality = px.bar(
            quality_summary,
            x="metric",
            y="coverage_pct",
            color="coverage_pct",
            text="coverage_pct",
            title="Data quality coverage across enriched events",
        )
        fig_quality.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        st.plotly_chart(fig_quality, use_container_width=True)
    with c2:
        source_quality = (
            filtered.groupby("source")[QUALITY_COLS + ["has_any_geo", "completeness_score"]]
            .mean()
            .mul(100)
            .round(1)
            .reset_index()
            .rename(columns={col: QUALITY_LABELS.get(col, col) for col in QUALITY_COLS})
            .rename(
                columns={
                    "has_any_geo": "any geo coverage",
                    "completeness_score": "overall completeness",
                }
            )
        )
        heatmap_df = source_quality.set_index("source")
        fig_heat = px.imshow(
            heatmap_df,
            text_auto=".1f",
            aspect="auto",
            color_continuous_scale="Blues",
            title="Quality coverage by source (%)",
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        daily_quality = (
            filtered.groupby("day")[["completeness_score", "has_any_geo", "has_text", "has_classify_conf"]]
            .mean()
            .reset_index()
            .rename(
                columns={
                    "completeness_score": "overall completeness",
                    "has_any_geo": "any geo coverage",
                    "has_text": "non-empty text",
                    "has_classify_conf": "classification confidence",
                }
            )
        )
        fig_quality_trend = px.line(
            daily_quality,
            x="day",
            y=[
                "overall completeness",
                "any geo coverage",
                "non-empty text",
                "classification confidence",
            ],
            title="Daily quality trend",
            labels={"value": "share", "variable": "metric"},
        )
        st.plotly_chart(fig_quality_trend, use_container_width=True)
    with c4:
        event_type_quality = (
            filtered[filtered["event_type"].isin(top_types)]
            .groupby("event_type")[QUALITY_COLS]
            .mean()
            .mul(100)
            .round(1)
            .rename(columns=QUALITY_LABELS)
        )
        fig_type_quality = px.imshow(
            event_type_quality,
            text_auto=".1f",
            aspect="auto",
            color_continuous_scale="GnBu",
            title="Quality coverage by top event types (%)",
        )
        st.plotly_chart(fig_type_quality, use_container_width=True)

    st.dataframe(source_quality, use_container_width=True)

with tabs[5]:
    if clustered.empty or cluster_terms.empty:
        st.info("Cluster analytics are not available yet - run the clustering step first.")
    else:
        cluster_summary = (
            clustered.groupby("cluster_id")
            .agg(
                events=("cluster_id", "size"),
                avg_severity=("severity", "mean"),
            )
            .reset_index()
            .sort_values("events", ascending=False)
        )

        dominant_type = (
            clustered.groupby(["cluster_id", "event_type"])
            .size()
            .reset_index(name="n")
            .sort_values(["cluster_id", "n", "event_type"], ascending=[True, False, True])
            .drop_duplicates("cluster_id")
            .rename(columns={"event_type": "dominant_event_type", "n": "dominant_type_events"})
        )

        top_terms = (
            cluster_terms.sort_values(["cluster_id", "n", "token"], ascending=[True, False, True])
            .groupby("cluster_id")
            .head(8)
            .groupby("cluster_id")["token"]
            .agg(lambda s: ", ".join(s.astype(str)))
            .reset_index(name="top_terms")
        )

        cluster_summary = (
            cluster_summary.merge(dominant_type, on="cluster_id", how="left")
            .merge(top_terms, on="cluster_id", how="left")
        )

        c1, c2 = st.columns(2)
        with c1:
            fig_clusters = px.bar(
                cluster_summary,
                x="cluster_id",
                y="events",
                color="avg_severity",
                hover_data=["dominant_event_type"],
                title="Cluster sizes and average severity",
            )
            st.plotly_chart(fig_clusters, use_container_width=True)
        with c2:
            cluster_event = (
                clustered.groupby(["cluster_id", "event_type"])
                .size()
                .reset_index(name="n")
            )
            pivot = (
                cluster_event.pivot(index="cluster_id", columns="event_type", values="n")
                .fillna(0)
            )
            fig_cluster_heat = px.imshow(
                pivot,
                text_auto=True,
                aspect="auto",
                color_continuous_scale="YlOrRd",
                title="Cluster x event-type intensity",
            )
            st.plotly_chart(fig_cluster_heat, use_container_width=True)

        st.dataframe(
            cluster_summary.round({"avg_severity": 3}),
            use_container_width=True,
        )
