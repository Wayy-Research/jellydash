"""Jelly Games — Leaderboards and badges."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from jellydash.analytics.games import ALL_GAMES
from jellydash.db import queries
from jellydash.ui.components import badge_icon
from jellydash.ui.helpers import get_db

st.set_page_config(page_title="Games | JellyDash", page_icon="🎮", layout="wide")
st.title("🎮 Jelly Games")

conn = get_db()

tabs = st.tabs([g.name for g in ALL_GAMES])

for tab, game in zip(tabs, ALL_GAMES):
    with tab:
        st.markdown(f"**{game.description}**")
        st.caption(
            f"Unit: {game.unit} | {'Lower' if game.ascending else 'Higher'} is better"
        )

        leaderboard = queries.get_game_leaderboard(conn, game.game_id, limit=20)

        if leaderboard:
            df = pd.DataFrame(leaderboard)

            # Add badge icons
            def _badge_col(row: pd.Series) -> str:  # type: ignore[type-arg]
                badges = queries.get_user_badges(conn, row["participant_id"])
                icons = [
                    badge_icon(b["badge_id"])
                    for b in badges
                    if b["game_id"] == game.game_id
                ]
                return " ".join(icons)

            df["badges"] = df.apply(_badge_col, axis=1)
            st.dataframe(
                df[["rank", "username", "score", "sample_size", "badges"]],
                use_container_width=True,
                hide_index=True,
            )

            # Score distribution
            st.subheader("Score Distribution")
            fig = px.histogram(df, x="score", nbins=15, labels={"score": game.unit})
            st.plotly_chart(fig, use_container_width=True)

            # Search for user
            username_search = st.text_input(
                "Find your rank", key=f"search_{game.game_id}"
            )
            if username_search:
                match = df[
                    df["username"].str.contains(username_search, case=False, na=False)
                ]
                if not match.empty:
                    st.write(match[["rank", "username", "score", "badges"]])
                else:
                    st.warning("User not found in this game's leaderboard.")
        else:
            st.info("No scores computed yet. Run a sync and refresh analytics.")

# Badge Gallery
st.divider()
st.subheader("🏅 Badge Gallery")

for game in ALL_GAMES:
    st.markdown(f"**{game.name}**")
    for tier, threshold in game.badges.items():
        direction = "<=" if game.ascending else ">="
        st.markdown(
            f"- {badge_icon(f'{game.game_id}_{tier}')} **{tier.title()}**: "
            f"{direction} {threshold} {game.unit}"
        )
