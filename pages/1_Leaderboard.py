"""Leaderboard — User rankings by engagement metrics."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from jellydash.analytics.rankings import get_ranked_users, get_rising_stars
from jellydash.db import queries
from jellydash.ui.helpers import get_db

st.set_page_config(page_title="Leaderboard | JellyDash", page_icon="🏆", layout="wide")
st.title("🏆 Leaderboard")

conn = get_db()

col1, col2 = st.columns(2)

with col1:
    metric = st.selectbox(
        "Rank by",
        [
            "total_views",
            "total_likes",
            "total_comments",
            "total_tips",
            "total_jellies",
            "avg_views",
            "avg_likes",
            "views_per_post",
            "jelly_score",
        ],
        format_func=lambda x: x.replace("_", " ").title(),
    )

# Get available topics for filter
with col2:
    topic_rows = queries.get_topics(conn, "all_time", limit=50)
    topic_options = ["All"] + [r["topic"] for r in topic_rows]
    topic = st.selectbox("Filter by topic", topic_options)

selected_topic = topic if topic != "All" else None
rows = get_ranked_users(conn, metric=metric, topic=selected_topic, limit=50)

if rows:
    df = pd.DataFrame(rows)
    display_cols = ["rank", "username", "full_name", metric, "total_jellies"]
    available = [c for c in display_cols if c in df.columns]
    st.dataframe(df[available], use_container_width=True, hide_index=True)

    # Expandable per-user details
    st.divider()
    st.subheader("User Details")
    usernames = df["username"].tolist()
    selected_user = st.selectbox("Select a user", usernames)

    if selected_user:
        user_row = df[df["username"] == selected_user].iloc[0]
        pid = user_row["participant_id"]

        user_jellies = queries.get_user_jellies(conn, pid, limit=5)
        if user_jellies:
            st.write(f"**Top 5 jellies by {selected_user}:**")
            jdf = pd.DataFrame(user_jellies)
            st.dataframe(
                jdf[
                    ["title", "all_views", "likes_count", "comments_count", "posted_at"]
                ],
                use_container_width=True,
                hide_index=True,
            )
else:
    st.info("No data available. Run a sync first.")

# Rising Stars section
st.divider()
st.subheader("Rising Stars")
st.caption("New creators (last 30 days) with 3+ posts and above-average views/post")

stars = get_rising_stars(conn, limit=20)
if stars:
    star_df = pd.DataFrame(stars)
    display = ["username", "full_name", "total_jellies", "views_per_post", "jelly_score", "first_seen_at"]
    available = [c for c in display if c in star_df.columns]
    st.dataframe(star_df[available], use_container_width=True, hide_index=True)
else:
    st.info("No rising stars found yet.")
