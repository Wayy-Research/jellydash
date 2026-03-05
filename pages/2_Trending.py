"""Trending — Topic analytics and transcript stats."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from jellydash.db import queries
from jellydash.ui.helpers import get_db

st.set_page_config(page_title="Trending | JellyDash", page_icon="📈", layout="wide")
st.title("📈 Trending Topics")

conn = get_db()

period = st.selectbox("Period", ["all_time", "30d", "7d", "24h"])

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

    # Topic table
    st.subheader("Top Topics")
    st.dataframe(
        df[["topic", "score"]].rename(columns={"score": "TF-IDF Score"}),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No topics computed yet. Run a sync and refresh analytics.")

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
