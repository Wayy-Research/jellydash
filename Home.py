"""JellyDash — Home: Platform overview dashboard."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from jellydash.db import queries
from jellydash.sync.background import (
    ensure_initial_sync,
    is_sync_running,
    start_background_sync,
)
from jellydash.ui.components import metric_cards
from jellydash.ui.helpers import get_db

logging.basicConfig(level=logging.INFO)

st.set_page_config(page_title="JellyDash", page_icon="🪼", layout="wide")
st.title("🪼 JellyDash — Platform Analytics")

DB_PATH = str(Path(__file__).resolve().parent / "data" / "jellydash.db")

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

# Quick links
st.divider()
st.subheader("Quick Links")
cols = st.columns(5)
cols[0].page_link("pages/1_Leaderboard.py", label="🏆 Leaderboard")
cols[1].page_link("pages/2_Trending.py", label="📈 Trending")
cols[2].page_link("pages/3_Games.py", label="🎮 Games")
cols[3].page_link("pages/4_Explorer.py", label="🔍 Explorer")
cols[4].page_link("pages/5_Sync.py", label="⚙️ Sync")
