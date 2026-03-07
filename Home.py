"""JellyDash — Home: Platform overview dashboard."""

from __future__ import annotations

import logging

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

from jellydash.db import queries
from jellydash.sync.background import (
    ensure_initial_sync,
    is_sync_running,
    start_background_sync,
)
from jellydash.ui.components import metric_cards
from jellydash.ui.helpers import get_db, get_db_path_str, hls_player

logging.basicConfig(level=logging.INFO)

st.set_page_config(page_title="JellyDash", page_icon="🪼", layout="wide")
st.title("🪼 JellyDash — Platform Analytics")

DB_PATH = get_db_path_str()

conn = get_db()

# Auto-sync: populate DB on first load, start background thread
if "initial_sync_done" not in st.session_state:
    with st.spinner("Loading data from JellyJelly API (first run)..."):
        result = ensure_initial_sync(DB_PATH)
        if result:
            st.toast(
                f"Initial sync: {result['detailed']} jellies loaded",
                icon="✅",
            )
    st.session_state["initial_sync_done"] = True

# Start background sync thread (runs every 15 min)
if not is_sync_running():
    start_background_sync(DB_PATH)

# Platform stats
stats = queries.get_platform_stats(conn)
metric_cards(stats)

sync_status = "🟢 Live" if is_sync_running() else "🔴 Stopped"
st.caption(f"Background sync: {sync_status} (refreshes every 15 min)")

st.divider()

# Jellies per day time series
st.subheader("Jellies Posted Per Day")
daily = queries.get_jellies_per_day(conn, limit=90)
if daily:
    df = pd.DataFrame(daily)
    df["day"] = pd.to_datetime(df["day"])
    df = df.sort_values("day")
    fig = px.line(
        df,
        x="day",
        y="count",
        labels={"day": "Date", "count": "Jellies"},
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data yet — initial sync is running...")

# Popular Jellies
st.divider()
st.subheader("Popular Jellies")
popular_rows = conn.execute(
    """
    SELECT id, title, all_views, likes_count, comments_count,
           thumbnail_url, hls_master, duration
    FROM jellies
    WHERE all_views > 0
    ORDER BY all_views DESC
    LIMIT 50
    """
).fetchall()
if popular_rows:
    for i, row in enumerate(popular_rows):
        j = dict(row)
        views = f"{j['all_views']:,}"
        likes = f"{j['likes_count']:,}"
        with st.expander(
            f"#{i+1} {j['title'][:80]} — {views} views, {likes} likes",
            expanded=False,
        ):
            mcols = st.columns([2, 1, 1, 1])
            mcols[0].metric("Views", views)
            mcols[1].metric("Likes", likes)
            mcols[2].metric("Comments", f"{j['comments_count']:,}")
            if j.get("thumbnail_url"):
                mcols[3].image(j["thumbnail_url"], width=120)
            if j.get("hls_master"):
                components.html(hls_player(j["hls_master"], player_id=f"home-{i}"), height=340)
            else:
                st.caption("No video stream available.")
else:
    st.info("No popular jellies yet — data loads after first sync.")

# Quick links
st.divider()
st.subheader("Quick Links")
cols = st.columns(6)
cols[0].page_link("pages/1_Leaderboard.py", label="🏆 Leaderboard")
cols[1].page_link("pages/2_Trending.py", label="📈 Trending")
cols[2].page_link("pages/3_Games.py", label="🎮 Games")
cols[3].page_link("pages/4_Explorer.py", label="🔍 Explorer")
cols[4].page_link("pages/5_Sync.py", label="⚙️ Sync")
cols[5].page_link("pages/6_Context.py", label="🧠 Context Layer")
