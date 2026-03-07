"""Sync — Admin page for triggering syncs and viewing status."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from jellydash.analytics.games import refresh_all_games
from jellydash.analytics.rankings import refresh_user_stats
from jellydash.analytics.topics import refresh_topics
from jellydash.db import queries
from jellydash.sync.background import get_last_error, is_sync_running
from jellydash.ui.helpers import get_db, get_db_path_str, run_async

st.set_page_config(page_title="Sync | JellyDash", page_icon="⚙️", layout="wide")
st.title("⚙️ Sync & Admin")

sync_status = "🟢 Live" if is_sync_running() else "🔴 Stopped"
st.caption(f"Background sync: {sync_status} (auto-refreshes every 15 min)")

last_err = get_last_error()
if last_err:
    with st.expander("Last sync error", expanded=True):
        st.code(last_err)

conn = get_db()

# Diagnostic
if st.button("🔍 Run API Diagnostic"):
    with st.spinner("Testing API connectivity..."):
        from jellydash.sync.scraper import run_diagnostic

        try:
            diag = run_async(run_diagnostic())
            st.json(diag)
        except Exception as exc:
            st.error(f"Diagnostic failed: {type(exc).__name__}: {exc}")

# Sync controls
col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("🔄 Run Full Sync", type="primary"):
        with st.spinner("Running full sync..."):
            from jellydash.sync.scraper import run_full_sync

            result = run_async(run_full_sync(conn))
            st.success(
                f"Sync complete: {result['detailed']} jellies fetched, "
                f"{result['errors']} errors"
            )
            if result.get("discovery_errors"):
                with st.expander(
                    f"Discovery errors ({len(result['discovery_errors'])})",
                    expanded=True,
                ):
                    for err in result["discovery_errors"]:
                        st.code(err)

with col2:
    if st.button("⚡ Run Incremental Sync"):
        with st.spinner("Running incremental sync..."):
            from jellydash.sync.scraper import run_incremental_sync

            result = run_async(run_incremental_sync(conn))
            st.success(
                f"Incremental sync: {result['detailed']} refreshed, "
                f"{result['errors']} errors"
            )

with col3:
    if st.button("📊 Refresh Analytics"):
        with st.spinner("Refreshing analytics..."):
            n_users = refresh_user_stats(conn)
            for period in ["all_time", "30d", "7d", "24h"]:
                refresh_topics(conn, period)
            game_results = refresh_all_games(conn)
            total_scores = (
                game_results["transcript_scores"]
                + game_results["aggregate_scores"]
            )
            st.success(
                f"Refreshed: {n_users} users, "
                f"{total_scores} scores, "
                f"{game_results['badges_awarded']} badges"
            )

with col4:
    if st.button("🌐 Full Date Sweep"):
        with st.spinner("Running full date sweep (this may take 15-30 min)..."):
            from jellydash.sync.scraper import run_full_sync

            result = run_async(run_full_sync(conn, use_date_sweep=True))
            st.success(
                f"Date sweep complete: {result['new_ids']} new IDs, "
                f"{result['detailed']} fetched, {result['errors']} errors"
            )
            if result.get("discovery_errors"):
                with st.expander(
                    f"Discovery errors ({len(result['discovery_errors'])})",
                    expanded=True,
                ):
                    for err in result["discovery_errors"]:
                        st.code(err)

# Sync history
st.divider()
st.subheader("Sync History")
runs = queries.get_sync_runs(conn, limit=20)
if runs:
    df = pd.DataFrame(runs)
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("No sync runs yet.")

# PostgreSQL sync
st.divider()
st.subheader("PostgreSQL Cloud Sync")

from jellydash.db.pg_sync import get_pg_url

pg_url = get_pg_url()
if pg_url:
    st.success(f"Connected to: `{pg_url.split('@')[1].split('/')[0] if '@' in pg_url else 'configured'}`")

    pg_col1, pg_col2 = st.columns(2)
    with pg_col1:
        if st.button("☁️ Push to wayyDB"):
            with st.spinner("Pushing data to PostgreSQL..."):
                from jellydash.db.pg_sync import push_to_pg

                try:
                    counts = push_to_pg(conn)
                    st.success(
                        f"Pushed: {counts.get('jellies', 0)} jellies, "
                        f"{counts.get('participants', 0)} users, "
                        f"{counts.get('transcripts', 0)} transcripts, "
                        f"{counts.get('transcript_segments', 0)} segments"
                    )
                except Exception as exc:
                    st.error(f"Push failed: {type(exc).__name__}: {exc}")

    with pg_col2:
        if st.button("🗄️ Init PG Schema"):
            with st.spinner("Creating jelly schema on wayyDB..."):
                from jellydash.db.pg_sync import init_pg_schema

                try:
                    init_pg_schema()
                    st.success("Schema initialized on wayyDB")
                except Exception as exc:
                    st.error(f"Schema init failed: {type(exc).__name__}: {exc}")
else:
    st.warning("Set `DATABASE_URL` env var to enable PostgreSQL sync")
    st.code(
        "export DATABASE_URL=postgresql://wayy:***@100.96.150.42:5432/wayydb",
        language="bash",
    )

# DB stats
st.divider()
st.subheader("Database Stats")

db_path = Path(get_db_path_str())
if db_path.exists():
    size_mb = db_path.stat().st_size / (1024 * 1024)
    st.write(f"**Database size:** {size_mb:.2f} MB")

stats = queries.get_platform_stats(conn)
cols = st.columns(4)
cols[0].metric("Total Jellies", stats.get("total_jellies", 0))
cols[1].metric("Total Users", stats.get("total_users", 0))
cols[2].metric("Total Views", f"{stats.get('total_views', 0):,}")
cols[3].metric("Last Sync", str(stats.get("last_sync", "Never"))[:16])
