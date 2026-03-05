"""Streamlit helper utilities — async bridge, DB caching."""

from __future__ import annotations

import asyncio
import shutil
import sqlite3
from pathlib import Path
from typing import Any, TypeVar

import streamlit as st

from jellydash.db.connection import get_connection
from jellydash.db.schema import create_tables

T = TypeVar("T")

_SEED_DB = Path(__file__).resolve().parent.parent.parent / "data" / "jellydash.db"
_LIVE_DB = Path("/tmp/jellydash.db")


def _get_db_path() -> Path:
    """Return a writable DB path, copying the seed DB if needed."""
    if not _LIVE_DB.exists() and _SEED_DB.exists():
        shutil.copy2(_SEED_DB, _LIVE_DB)
    return _LIVE_DB if _LIVE_DB.exists() else _SEED_DB


def run_async(coro: Any) -> Any:
    """Run an async coroutine from synchronous Streamlit code."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result()
    return asyncio.run(coro)


@st.cache_resource
def get_db() -> sqlite3.Connection:
    """Get a cached database connection, creating tables if needed."""
    db_path = _get_db_path()
    conn = get_connection(db_path)
    create_tables(conn)
    return conn


def get_db_path_str() -> str:
    """Return the writable DB path as a string (for background sync)."""
    return str(_get_db_path())


def format_number(n: int | float) -> str:
    """Format a number with K/M suffixes."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))
