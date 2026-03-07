"""Jelly Context Layer — text-based video search across transcripts."""

from __future__ import annotations

import html
import os
import re

import streamlit as st
import streamlit.components.v1 as components

from jellydash.context.groq_search import (
    get_popular_jellies,
    semantic_search,
    text_search,
)
from jellydash.context.segmenter import build_all_segments
from jellydash.ui.helpers import get_db, hls_player

st.set_page_config(
    page_title="Context Layer | JellyDash", page_icon="🧠", layout="wide"
)
st.title("🧠 Jelly Context Layer")
st.caption("Search transcripts, find moments, watch at the exact timestamp.")

conn = get_db()

# Build segments on first visit (FTS5 triggers keep the index in sync)
if "segments_built" not in st.session_state:
    with st.spinner("Indexing transcript segments..."):
        n = build_all_segments(conn)
    if n > 0:
        st.toast(f"Indexed {n} new transcript segments", icon="✅")
    st.session_state["segments_built"] = True

# Groq status
has_groq = bool(os.environ.get("GROQ_API_KEY"))
if has_groq:
    st.sidebar.success("Groq API: Connected")
    st.sidebar.caption("Semantic reranking enabled")
else:
    st.sidebar.warning("Set GROQ_API_KEY for semantic reranking")
    st.sidebar.caption("Using text search only")

# --- Search ---
st.subheader("Search Videos by Transcript")

query = st.text_input(
    "What are you looking for?",
    placeholder="e.g. bitcoin price prediction, startup funding, AI regulation...",
)

col1, col2 = st.columns([3, 1])
with col2:
    top_k = st.slider("Results", min_value=3, max_value=25, value=10)

if query:
    with st.spinner("Searching transcripts..."):
        if has_groq:
            results = semantic_search(conn, query, top_k=top_k)
        else:
            results = text_search(conn, query, limit=top_k)

    if not results:
        st.info("No matching segments found. Try different keywords.")
    else:
        st.write(f"**{len(results)} results** for *\"{query}\"*")

        for i, r in enumerate(results):
            start_sec = int(r["start_time"])
            end_sec = int(r["end_time"])
            minutes = start_sec // 60
            seconds = start_sec % 60
            timestamp = f"{minutes}:{seconds:02d}"

            with st.expander(
                f"#{i+1} — {r['title']} @ {timestamp} "
                f"({r['all_views']:,} views)",
                expanded=(i == 0),
            ):
                # Segment text with highlight
                st.markdown(f"**Segment** ({r['start_time']:.0f}s – {r['end_time']:.0f}s)")
                display_text = r["text"]
                for word in query.lower().split():
                    if len(word) > 2:
                        pattern = re.compile(re.escape(word), re.IGNORECASE)
                        display_text = pattern.sub(
                            lambda m: f"**:orange[{m.group()}]**", display_text
                        )
                st.markdown(display_text)

                # Video player
                hls_url = r.get("hls_master")
                if hls_url:
                    st.markdown("---")
                    st.markdown(f"▶️ **Play from {timestamp}**")
                    components.html(
                        hls_player(hls_url, start_sec=start_sec, player_id=f"ctx-{i}"),
                        height=340,
                    )
                else:
                    st.caption("No video stream available for this jelly.")

                # Metadata row
                mcols = st.columns(4)
                mcols[0].metric("Views", f"{r['all_views']:,}")
                mcols[1].metric("Likes", f"{r['likes_count']:,}")
                if r.get("thumbnail_url"):
                    mcols[3].image(r["thumbnail_url"], width=120)

st.divider()

# --- Popular Jellies ---
st.subheader("Popular Jellies")
st.caption("Most viewed jellies with transcripts available for search.")

popular = get_popular_jellies(conn, limit=50)
if popular:
    for i, j in enumerate(popular):
        has_tx = "✅" if j.get("has_transcript") else "—"
        views = f"{j['all_views']:,}"
        likes = f"{j['likes_count']:,}"
        with st.expander(
            f"#{i+1} {j['title'][:80]} — {views} views, {likes} likes (Transcript: {has_tx})",
            expanded=False,
        ):
            mcols = st.columns([2, 1, 1])
            mcols[0].metric("Views", views)
            mcols[1].metric("Likes", likes)
            if j.get("thumbnail_url"):
                mcols[2].image(j["thumbnail_url"], width=120)
            if j.get("hls_master"):
                components.html(
                    hls_player(j["hls_master"], player_id=f"pop-{i}"),
                    height=340,
                )
            else:
                st.caption("No video stream available.")
else:
    st.info("No jellies found. Run a sync from the Sync page first.")
