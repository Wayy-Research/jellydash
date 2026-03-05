"""Reusable Streamlit widgets for JellyDash."""

from __future__ import annotations

import sqlite3
from typing import Any

import pandas as pd
import streamlit as st

from jellydash.db import queries


def metric_cards(stats: dict[str, Any]) -> None:
    """Render a row of st.metric cards for platform stats."""
    cols = st.columns(5)
    cols[0].metric("Total Jellies", f"{stats.get('total_jellies', 0):,}")
    cols[1].metric("Total Users", f"{stats.get('total_users', 0):,}")
    cols[2].metric("Total Views", f"{stats.get('total_views', 0):,}")
    cols[3].metric("Total Likes", f"{stats.get('total_likes', 0):,}")
    last_sync = stats.get("last_sync", "Never")
    cols[4].metric("Last Sync", str(last_sync)[:16] if last_sync else "Never")


def user_leaderboard(
    conn: sqlite3.Connection,
    metric: str = "total_views",
    topic: str | None = None,
    limit: int = 50,
) -> None:
    """Render a user leaderboard dataframe."""
    from jellydash.analytics.rankings import get_ranked_users

    rows = get_ranked_users(conn, metric=metric, topic=topic, limit=limit)
    if not rows:
        st.info("No data available. Run a sync first.")
        return

    df = pd.DataFrame(rows)
    display_cols = ["rank", "username", "full_name", metric, "total_jellies"]
    available = [c for c in display_cols if c in df.columns]
    st.dataframe(df[available], use_container_width=True, hide_index=True)


def jelly_card(jelly: dict[str, Any], conn: sqlite3.Connection) -> None:
    """Render a jelly detail card."""
    st.subheader(jelly.get("title", "Untitled"))

    cols = st.columns(4)
    cols[0].metric("Views", f"{jelly.get('all_views', 0):,}")
    cols[1].metric("Likes", f"{jelly.get('likes_count', 0):,}")
    cols[2].metric("Comments", f"{jelly.get('comments_count', 0):,}")
    cols[3].metric("Tips", f"${jelly.get('tips_total', 0):.2f}")

    if jelly.get("thumbnail_url"):
        st.image(jelly["thumbnail_url"], width=300)

    transcript = queries.get_transcript(conn, jelly["id"])
    if transcript and transcript.get("full_text"):
        with st.expander("Transcript"):
            st.text(transcript["full_text"][:2000])


def badge_icon(badge_id: str) -> str:
    """Return an emoji for a badge tier."""
    if "gold" in badge_id:
        return "🥇"
    if "silver" in badge_id:
        return "🥈"
    if "bronze" in badge_id:
        return "🥉"
    return "🏅"
