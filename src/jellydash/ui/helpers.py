"""Streamlit helper utilities — async bridge, DB caching."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any, TypeVar

import streamlit as st

from jellydash.db.connection import get_connection
from jellydash.db.schema import create_tables

T = TypeVar("T")

_DB_PATH = (
    Path(__file__).resolve().parent.parent.parent.parent / "data" / "jellydash.db"
)


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
    conn = get_connection(_DB_PATH)
    create_tables(conn)
    return conn


def format_number(n: int | float) -> str:
    """Format a number with K/M suffixes."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))
