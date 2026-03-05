"""Background sync thread for continuous data capture."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_sync_thread: threading.Thread | None = None
_sync_lock = threading.Lock()

SYNC_INTERVAL_SECONDS = 15 * 60  # 15 minutes


def _run_sync_loop(
    db_path: str,
    interval: int = SYNC_INTERVAL_SECONDS,
) -> None:
    """Run sync in a loop forever (runs in background thread)."""
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
            result = asyncio.run(run_full_sync(conn, max_pages=10))
            logger.info("Sync done: %s", result)

            # Refresh analytics
            refresh_user_stats(conn)
            for period in ["all_time", "30d", "7d", "24h"]:
                refresh_topics(conn, period)
            refresh_all_games(conn)
            logger.info("Analytics refreshed")

            conn.close()
        except Exception:
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
    result: dict[str, Any] = asyncio.run(run_full_sync(conn, max_pages=10))

    refresh_user_stats(conn)
    for period in ["all_time", "30d", "7d", "24h"]:
        refresh_topics(conn, period)
    refresh_all_games(conn)

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
