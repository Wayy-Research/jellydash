"""Trending — Groq-powered topic analytics."""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

from jellydash.analytics.topics import extract_topics_incremental, refresh_topics
from jellydash.db import queries
from jellydash.ui.helpers import get_db, hls_player

st.set_page_config(page_title="Trending | JellyDash", page_icon="📈", layout="wide")
st.title("📈 Trending Topics")

conn = get_db()
has_groq = bool(os.environ.get("GROQ_API_KEY"))

# Controls
col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    period = st.selectbox("Period", ["all_time", "30d", "7d", "24h"])
with col2:
    if has_groq:
        if st.button("🔄 Refresh Topics"):
            with st.spinner("Extracting topics via Groq..."):
                n = refresh_topics(conn, period)
                st.toast(f"Refreshed {n} trending topics", icon="✅")
                st.rerun()
    else:
        st.warning("Set GROQ_API_KEY")
with col3:
    if has_groq:
        if st.button("⚡ Extract New"):
            with st.spinner("Processing unextracted transcripts..."):
                result = extract_topics_incremental(conn, max_batches=3)
                msg = (
                    f"Processed {result.get('processed', 0)} jellies, "
                    f"{result.get('topics_stored', 0)} topics extracted"
                )
                remaining = result.get("remaining", 0)
                if remaining > 0:
                    msg += f" ({remaining} remaining)"
                st.toast(msg, icon="✅")
                st.rerun()

# Extraction progress
unprocessed = conn.execute(
    """
    SELECT COUNT(*) as c FROM transcripts t
    WHERE t.word_count > 20
    AND t.jelly_id NOT IN (SELECT DISTINCT jelly_id FROM jelly_topics)
    """
).fetchone()
total_with_topics = conn.execute(
    "SELECT COUNT(DISTINCT jelly_id) as c FROM jelly_topics"
).fetchone()

p1, p2 = st.columns(2)
p1.metric("Jellies with Topics", f"{total_with_topics['c']:,}" if total_with_topics else "0")
p2.metric("Awaiting Extraction", f"{unprocessed['c']:,}" if unprocessed else "0")

st.divider()

# Topic display
topics = queries.get_topics(conn, period, limit=30)

if topics:
    df = pd.DataFrame(topics)

    # Treemap
    st.subheader("Topic Heatmap")
    fig = px.treemap(
        df,
        path=["topic"],
        values="score",
        color="score",
        color_continuous_scale="YlOrRd",
    )
    fig.update_layout(margin=dict(t=30, l=0, r=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

    # Topic table with drill-down
    st.subheader("Top Topics")
    st.dataframe(
        df[["topic", "score"]].rename(columns={"score": "Relevance Score"}),
        use_container_width=True,
        hide_index=True,
    )

    # Topic drill-down: show jellies for a selected topic
    st.divider()
    st.subheader("Topic Drill-Down")
    selected_topic = st.selectbox(
        "Select a topic to see related jellies",
        options=df["topic"].tolist(),
    )
    if selected_topic:
        related = conn.execute(
            """
            SELECT j.id, j.title, j.all_views, j.likes_count, j.posted_at,
                   j.hls_master, j.thumbnail_url, jt.relevance
            FROM jelly_topics jt
            JOIN jellies j ON j.id = jt.jelly_id
            WHERE jt.topic = ?
            ORDER BY jt.relevance DESC
            LIMIT 30
            """,
            (selected_topic,),
        ).fetchall()
        if related:
            for i, row in enumerate(related):
                r = dict(row)
                views = f"{r['all_views']:,}"
                likes = f"{r['likes_count']:,}"
                relevance = f"{r['relevance']:.0%}"
                with st.expander(
                    f"#{i+1} {r['title'][:80]} — {views} views, "
                    f"relevance: {relevance}",
                    expanded=False,
                ):
                    mcols = st.columns([2, 1, 1, 1])
                    mcols[0].metric("Views", views)
                    mcols[1].metric("Likes", likes)
                    mcols[2].metric("Relevance", relevance)
                    if r.get("thumbnail_url"):
                        mcols[3].image(r["thumbnail_url"], width=120)
                    if r.get("hls_master"):
                        components.html(
                            hls_player(r["hls_master"], player_id=f"trend-{i}"),
                            height=340,
                        )
                    else:
                        st.caption("No video stream available.")
        else:
            st.info("No jellies tagged with this topic yet.")
else:
    if has_groq:
        st.info("No topics computed yet. Click 'Extract New' to start topic extraction.")
    else:
        st.info(
            "No topics computed. Set GROQ_API_KEY and click 'Extract New' to start."
        )

# Platform transcript stats
st.divider()
st.subheader("Platform Transcript Stats")

stats_row = conn.execute(
    """
    SELECT
        COUNT(*) as total_transcripts,
        AVG(word_count) as avg_words,
        AVG(duration) as avg_duration
    FROM transcripts
    WHERE word_count > 0
    """
).fetchone()

if stats_row and stats_row["total_transcripts"] > 0:
    cols = st.columns(3)
    cols[0].metric("Transcripts", f"{stats_row['total_transcripts']:,}")
    cols[1].metric("Avg Words", f"{stats_row['avg_words']:.0f}")
    cols[2].metric("Avg Duration", f"{stats_row['avg_duration']:.1f}s")
