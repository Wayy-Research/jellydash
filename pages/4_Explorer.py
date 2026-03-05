"""Explorer — Jelly and user drill-down."""

from __future__ import annotations

import streamlit as st

from jellydash.analytics.games import ALL_GAMES, FILLER_PATTERN
from jellydash.db import queries
from jellydash.ui.components import badge_icon, jelly_card
from jellydash.ui.helpers import get_db

st.set_page_config(page_title="Explorer | JellyDash", page_icon="🔍", layout="wide")
st.title("🔍 Explorer")

conn = get_db()

search_query = st.text_input("Search transcripts or titles")

if search_query:
    results = queries.search_transcripts(conn, search_query)

    if results:
        st.write(f"Found {len(results)} results")
        for r in results[:20]:
            with st.expander(f"{r['title']} — {r['all_views']:,} views"):
                jelly = queries.get_jelly_by_id(conn, r["jelly_id"])
                if jelly:
                    jelly_card(jelly, conn)
    else:
        st.info("No results found.")

st.divider()

# User profile view
st.subheader("User Profile")
all_stats = queries.get_all_user_stats(conn)
if all_stats:
    usernames = [s["username"] for s in all_stats]
    selected = st.selectbox("Select user", usernames)

    if selected:
        user = next((s for s in all_stats if s["username"] == selected), None)
        if user:
            pid = user["participant_id"]

            # Stats
            cols = st.columns(5)
            cols[0].metric("Jellies", user["total_jellies"])
            cols[1].metric("Total Views", f"{user['total_views']:,}")
            cols[2].metric("Total Likes", f"{user['total_likes']:,}")
            cols[3].metric("Avg Views", f"{user['avg_views']:.0f}")
            cols[4].metric("Tips", f"${user['total_tips']:.2f}")

            # Badges
            badges = queries.get_user_badges(conn, pid)
            if badges:
                st.write(
                    "**Badges:** "
                    + " ".join(
                        f"{badge_icon(b['badge_id'])} {b['badge_id']}" for b in badges
                    )
                )

            # Game scores
            for game in ALL_GAMES:
                lb = queries.get_game_leaderboard(conn, game.game_id, limit=200)
                user_score = next((r for r in lb if r["participant_id"] == pid), None)
                if user_score:
                    st.write(
                        f"**{game.name}:** Rank #{user_score['rank']} "
                        f"(Score: {user_score['score']:.2f} {game.unit})"
                    )

            # User's jellies
            st.divider()
            st.subheader("Recent Jellies")
            user_jellies = queries.get_user_jellies(conn, pid, limit=10)
            if user_jellies:
                for j in user_jellies:
                    with st.expander(f"{j['title']} — {j['all_views']:,} views"):
                        jelly_card(j, conn)
else:
    st.info("No users found. Run a sync first.")

# Transcript viewer with filler highlighting
st.divider()
st.subheader("Transcript Viewer")

jelly_id_input = st.text_input("Enter Jelly ID to view transcript")
if jelly_id_input:
    transcript = queries.get_transcript(conn, jelly_id_input)
    if transcript and transcript.get("full_text"):
        text = transcript["full_text"]
        # Highlight fillers
        highlighted = FILLER_PATTERN.sub(lambda m: f"**:red[{m.group()}]**", text)
        st.markdown(highlighted)
        st.caption(
            f"Words: {transcript['word_count']} | "
            f"Duration: {transcript['duration']:.1f}s"
        )
    else:
        st.warning("No transcript found for this ID.")
