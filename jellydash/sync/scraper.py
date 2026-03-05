"""Async scraper: discover IDs → fetch details → store in SQLite."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import Any

from jellyjelly.client import JellyClient
from jellyjelly.models import Jelly, JellyDetail
from jellyjelly.search import search_all_pages

from jellydash.db.queries import (
    finish_sync_run,
    get_existing_ids,
    get_stale_ids,
    start_sync_run,
    upsert_jelly_detail,
)

logger = logging.getLogger(__name__)

SEARCH_QUERIES: list[str] = [
    "",
    "fintech",
    "crypto",
    "legal",
    "AI",
    "startup",
    "web3",
    "politics",
    "sports",
    "music",
    "tech",
    "business",
    "comedy",
    "news",
]


async def discover_jelly_ids(
    client: JellyClient,
    existing_ids: set[str],
    max_pages_per_query: int = 5,
    page_size: int = 100,
) -> list[str]:
    """Run broad searches to discover jelly IDs not yet in the DB."""
    found: set[str] = set()

    for query in SEARCH_QUERIES:
        try:
            jellies: list[Jelly] = await search_all_pages(
                client, query, max_pages=max_pages_per_query, page_size=page_size
            )
            for j in jellies:
                if j.id not in existing_ids:
                    found.add(j.id)
            logger.info("Query %r: found %d jellies", query, len(jellies))
        except Exception:
            logger.exception("Error searching query %r", query)

    return list(found)


async def fetch_details(
    client: JellyClient,
    jelly_ids: list[str],
    conn: sqlite3.Connection,
    concurrency: int = 3,
) -> tuple[int, int]:
    """Fetch full details for a list of jelly IDs and upsert to DB.

    Returns:
        Tuple of (successful, errors).
    """
    sem = asyncio.Semaphore(concurrency)
    success = 0
    errors = 0

    async def _fetch_one(jid: str) -> None:
        nonlocal success, errors
        async with sem:
            try:
                detail: JellyDetail = await client.get_jelly(jid)
                upsert_jelly_detail(conn, detail)
                success += 1
                if success % 50 == 0:
                    logger.info("Fetched %d/%d details", success, len(jelly_ids))
            except Exception:
                logger.exception("Error fetching jelly %s", jid)
                errors += 1

    await asyncio.gather(*[_fetch_one(jid) for jid in jelly_ids])
    return success, errors


async def run_full_sync(
    conn: sqlite3.Connection,
    client: JellyClient | None = None,
    max_pages: int = 5,
    concurrency: int = 3,
) -> dict[str, Any]:
    """Run a full discovery + detail sync.

    Returns:
        Dict with sync run stats.
    """
    run_id = start_sync_run(conn, strategy="full")
    own_client = client is None

    try:
        if client is None:
            client = JellyClient()
            await client.__aenter__()

        existing = get_existing_ids(conn)
        new_ids = await discover_jelly_ids(
            client, existing, max_pages_per_query=max_pages
        )
        # Also re-fetch stale entries
        stale_ids = get_stale_ids(conn, max_age_hours=24)
        all_ids = list(set(new_ids) | set(stale_ids))

        logger.info(
            "Sync: %d new, %d stale, %d total to fetch",
            len(new_ids),
            len(stale_ids),
            len(all_ids),
        )

        detailed, errs = await fetch_details(client, all_ids, conn, concurrency)
        finish_sync_run(
            conn,
            run_id,
            status="completed",
            jellies_found=len(new_ids),
            jellies_detailed=detailed,
            errors=errs,
        )
        return {
            "run_id": run_id,
            "new_ids": len(new_ids),
            "stale_ids": len(stale_ids),
            "detailed": detailed,
            "errors": errs,
        }
    except Exception:
        finish_sync_run(conn, run_id, status="failed", errors=1)
        raise
    finally:
        if own_client and client is not None:
            await client.close()


async def run_incremental_sync(
    conn: sqlite3.Connection,
    client: JellyClient | None = None,
    concurrency: int = 3,
) -> dict[str, Any]:
    """Run an incremental sync — only re-fetch stale entries."""
    run_id = start_sync_run(conn, strategy="incremental")
    own_client = client is None

    try:
        if client is None:
            client = JellyClient()
            await client.__aenter__()

        stale_ids = get_stale_ids(conn, max_age_hours=12)
        logger.info("Incremental sync: %d stale to refresh", len(stale_ids))

        detailed, errs = await fetch_details(client, stale_ids, conn, concurrency)
        finish_sync_run(
            conn,
            run_id,
            status="completed",
            jellies_found=0,
            jellies_detailed=detailed,
            errors=errs,
        )
        return {
            "run_id": run_id,
            "stale_ids": len(stale_ids),
            "detailed": detailed,
            "errors": errs,
        }
    except Exception:
        finish_sync_run(conn, run_id, status="failed", errors=1)
        raise
    finally:
        if own_client and client is not None:
            await client.close()
