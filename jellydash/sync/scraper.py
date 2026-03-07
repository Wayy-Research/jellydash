"""Async scraper: discover IDs → fetch details → store in SQLite."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
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
    page_size: int = 50,
) -> tuple[list[str], list[str]]:
    """Run broad searches to discover jelly IDs not yet in the DB.

    Returns:
        Tuple of (new_ids, discovery_errors).
    """
    found: set[str] = set()
    errors: list[str] = []

    for query in SEARCH_QUERIES:
        try:
            jellies: list[Jelly] = await search_all_pages(
                client, query, max_pages=max_pages_per_query, page_size=page_size
            )
            for j in jellies:
                if j.id not in existing_ids:
                    found.add(j.id)
            logger.info("Query %r: found %d jellies", query, len(jellies))
        except Exception as exc:
            err_msg = f"Query {query!r}: {type(exc).__name__}: {exc}"
            errors.append(err_msg)
            logger.exception("Error searching query %r", query)

    return list(found), errors


async def discover_by_date_sweep(
    client: JellyClient,
    existing_ids: set[str],
    start: str = "2024-01-01",
    window_days: int = 7,
    max_pages_per_window: int = 10,
    page_size: int = 50,
) -> tuple[list[str], list[str]]:
    """Walk backward from today in weekly windows to discover all jellies.

    Args:
        client: Active JellyClient instance.
        existing_ids: IDs already in the DB (skip these).
        start: Earliest date to sweep (YYYY-MM-DD).
        window_days: Size of each date window in days.
        max_pages_per_window: Max pages to paginate per window.
        page_size: Results per page.

    Returns:
        Tuple of (new_ids, errors).
    """
    found: set[str] = set()
    errors: list[str] = []

    end_dt = datetime.now(timezone.utc).date()
    start_dt = datetime.strptime(start, "%Y-%m-%d").date()
    window = timedelta(days=window_days)

    if end_dt <= start_dt:
        logger.warning(
            "Date sweep: end %s <= start %s, nothing to sweep", end_dt, start_dt
        )
        return list(found), errors

    cursor = end_dt
    while cursor > start_dt:
        w_start = max(cursor - window, start_dt)
        w_end = cursor

        try:
            jellies: list[Jelly] = await search_all_pages(
                client,
                "",
                max_pages=max_pages_per_window,
                page_size=page_size,
                start_date=w_start.isoformat(),
                end_date=w_end.isoformat(),
            )
            new_in_window = 0
            for j in jellies:
                if j.id not in existing_ids:
                    found.add(j.id)
                    new_in_window += 1
            logger.info(
                "Date sweep %s → %s: %d results, %d new",
                w_start, w_end, len(jellies), new_in_window,
            )
        except Exception as exc:
            err_msg = f"Date sweep {w_start}→{w_end}: {type(exc).__name__}: {exc}"
            errors.append(err_msg)
            logger.exception("Error in date sweep window %s→%s", w_start, w_end)

        cursor = w_start

    return list(found), errors


async def run_diagnostic(
    client: JellyClient | None = None,
) -> dict[str, Any]:
    """Run a quick diagnostic to test API connectivity.

    Returns a dict with test results for debugging sync issues.
    """
    own_client = client is None
    results: dict[str, Any] = {}

    try:
        if client is None:
            client = JellyClient()
            await client.__aenter__()

        # Test 1: simple search
        try:
            resp = await client.search("", page=1, page_size=1)
            results["search_ok"] = True
            results["search_count"] = len(resp.jellies)
            if resp.jellies:
                results["first_id"] = resp.jellies[0].id
        except Exception as exc:
            results["search_ok"] = False
            results["search_error"] = f"{type(exc).__name__}: {exc}"

        # Test 2: get a specific jelly (if search worked)
        if results.get("first_id"):
            try:
                detail = await client.get_jelly(results["first_id"])
                results["detail_ok"] = True
                results["detail_title"] = detail.title[:80] if detail.title else None
            except Exception as exc:
                results["detail_ok"] = False
                results["detail_error"] = f"{type(exc).__name__}: {exc}"

    except Exception as exc:
        results["client_error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if own_client and client is not None:
            await client.close()

    return results


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
    use_date_sweep: bool = False,
) -> dict[str, Any]:
    """Run a full discovery + detail sync.

    Args:
        conn: Database connection.
        client: Optional JellyClient (created if None).
        max_pages: Max pages per keyword query.
        concurrency: Concurrent detail fetches.
        use_date_sweep: If True, also walk date windows to discover all jellies.

    Returns:
        Dict with sync run stats.
    """
    strategy = "date_sweep" if use_date_sweep else "full"
    run_id = start_sync_run(conn, strategy=strategy)
    own_client = client is None

    try:
        if client is None:
            client = JellyClient()
            await client.__aenter__()

        existing = get_existing_ids(conn)
        new_ids, discovery_errors = await discover_jelly_ids(
            client, existing, max_pages_per_query=max_pages
        )

        # Date sweep for full coverage
        if use_date_sweep:
            sweep_ids, sweep_errors = await discover_by_date_sweep(
                client, existing | set(new_ids)
            )
            new_ids = list(set(new_ids) | set(sweep_ids))
            discovery_errors.extend(sweep_errors)
        # Also re-fetch stale entries
        stale_ids = get_stale_ids(conn, max_age_hours=24)
        all_ids = list(set(new_ids) | set(stale_ids))

        logger.info(
            "Sync: %d new, %d stale, %d total to fetch, "
            "%d discovery errors",
            len(new_ids),
            len(stale_ids),
            len(all_ids),
            len(discovery_errors),
        )

        detailed, errs = await fetch_details(client, all_ids, conn, concurrency)
        total_errors = errs + len(discovery_errors)
        finish_sync_run(
            conn,
            run_id,
            status="completed",
            jellies_found=len(new_ids),
            jellies_detailed=detailed,
            errors=total_errors,
        )
        return {
            "run_id": run_id,
            "new_ids": len(new_ids),
            "stale_ids": len(stale_ids),
            "detailed": detailed,
            "errors": total_errors,
            "discovery_errors": discovery_errors,
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
