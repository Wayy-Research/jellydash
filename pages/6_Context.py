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
from jellydash.ui.helpers import get_db

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

                # Video player — use hls.js for cross-browser HLS support
                hls_url = r.get("hls_master")
                if hls_url:
                    st.markdown("---")
                    st.markdown(f"▶️ **Play from {timestamp}**")
                    safe_url = html.escape(hls_url, quote=True)
                    player_html = f"""
                    <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
                    <video id="player-{i}" controls width="100%" height="300"
                           style="background:#000; border-radius:8px;"></video>
                    <script>
                    (function() {{
                        var video = document.getElementById('player-{i}');
                        var url = "{safe_url}";
                        if (Hls.isSupported()) {{
                            var hls = new Hls();
                            hls.loadSource(url);
                            hls.attachMedia(video);
                            hls.on(Hls.Events.MANIFEST_PARSED, function() {{
                                video.currentTime = {start_sec};
                            }});
                        }} else if (video.canPlayType('application/vnd.apple.mpegurl')) {{
                            video.src = url;
                            video.addEventListener('loadedmetadata', function() {{
                                video.currentTime = {start_sec};
                            }});
                        }}
                    }})();
                    </script>
                    """
                    components.html(player_html, height=340)
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

popular = get_popular_jellies(conn, limit=20)
if popular:
    for j in popular:
        has_tx = "✅" if j.get("has_transcript") else "—"
        col1, col2, col3, col4 = st.columns([4, 1, 1, 1])
        with col1:
            st.write(f"**{j['title'][:80]}**")
        with col2:
            st.write(f"{j['all_views']:,} views")
        with col3:
            st.write(f"{j['likes_count']:,} likes")
        with col4:
            st.write(f"Transcript: {has_tx}")
else:
    st.info("No jellies found. Run a sync from the Sync page first.")
