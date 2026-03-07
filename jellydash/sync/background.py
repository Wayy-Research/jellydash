"""Background sync thread for continuous data capture."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import traceback
from typing import Any

logger = logging.getLogger(__name__)

_sync_thread: threading.Thread | None = None
_sync_lock = threading.Lock()
_last_error: str | None = None

SYNC_INTERVAL_SECONDS = 15 * 60  # 15 minutes


def _safe_async_run(coro: Any) -> Any:
    """Run async code safely, creating a new event loop if needed."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # We're in a thread with an existing loop — make a new one
        new_loop = asyncio.new_event_loop()
        try:
            return new_loop.run_until_complete(coro)
        finally:
            new_loop.close()
    return asyncio.run(coro)


def _run_sync_loop(
    db_path: str,
    interval: int = SYNC_INTERVAL_SECONDS,
) -> None:
    """Run sync in a loop forever (runs in background thread)."""
    global _last_error

    from jellydash.analytics.games import refresh_all_games
    from jellydash.analytics.rankings import refresh_user_stats
    from jellydash.analytics.topics import refresh_topics
    from jellydash.db.connection import get_connection
    from jellydash.db.schema import create_tables
    from jellydash.sync.scraper import run_full_sync

    while True:
        try:
            conn = get_connection(db_path)
            create_tables(conn)

            logger.info("Background sync starting...")
            result = _safe_async_run(run_full_sync(conn, max_pages=10))
            logger.info("Sync done: %s", result)
            _last_error = None

            # Refresh analytics
            refresh_user_stats(conn)
            for period in ["all_time", "30d", "7d", "24h"]:
                refresh_topics(conn, period)
            refresh_all_games(conn)
            logger.info("Analytics refreshed")

            # Build context layer segments
            from jellydash.context.segmenter import build_all_segments

            n_segs = build_all_segments(conn)
            if n_segs > 0:
                logger.info("Built %d new transcript segments", n_segs)

            # Push to PostgreSQL if configured
            from jellydash.db.pg_sync import get_pg_url

            if get_pg_url():
                try:
                    from jellydash.db.pg_sync import push_to_pg

                    push_to_pg(conn)
                    logger.info("Pushed data to PostgreSQL")
                except Exception:
                    logger.exception("PG push failed (non-fatal)")

            conn.close()
        except Exception:
            _last_error = traceback.format_exc()
            logger.exception("Background sync error")

        time.sleep(interval)


def ensure_initial_sync(db_path: str) -> dict[str, Any] | None:
    """Run one sync if the DB is empty. Returns result or None."""
    from jellydash.analytics.games import refresh_all_games
    from jellydash.analytics.rankings import refresh_user_stats
    from jellydash.analytics.topics import refresh_topics
    from jellydash.db.connection import get_connection
    from jellydash.db.schema import create_tables
    from jellydash.sync.scraper import run_full_sync

    conn = get_connection(db_path)
    create_tables(conn)

    row = conn.execute("SELECT COUNT(*) as c FROM jellies").fetchone()
    count = row["c"] if row else 0

    if count > 0:
        conn.close()
        return None

    logger.info("DB empty — running initial sync...")
    result: dict[str, Any] = _safe_async_run(run_full_sync(conn, max_pages=10))

    refresh_user_stats(conn)
    for period in ["all_time", "30d", "7d", "24h"]:
        refresh_topics(conn, period)
    refresh_all_games(conn)

    # Build context layer segments
    from jellydash.context.segmenter import build_all_segments

    build_all_segments(conn)

    conn.close()
    return result


def start_background_sync(db_path: str) -> bool:
    """Start the background sync thread if not already running.

    Returns True if a new thread was started.
    """
    global _sync_thread

    with _sync_lock:
        if _sync_thread is not None and _sync_thread.is_alive():
            return False

        _sync_thread = threading.Thread(
            target=_run_sync_loop,
            args=(db_path,),
            daemon=True,
            name="jellydash-sync",
        )
        _sync_thread.start()
        logger.info("Background sync thread started")
        return True


def is_sync_running() -> bool:
    """Check if the background sync thread is alive."""
    return _sync_thread is not None and _sync_thread.is_alive()


def get_last_error() -> str | None:
    """Return the last sync error traceback, if any."""
    return _last_error
