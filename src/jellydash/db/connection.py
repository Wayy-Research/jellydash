"""SQLite connection helper with WAL mode."""

from __future__ import annotations

import sqlite3
from pathlib import Path

_DEFAULT_DB_PATH = (
    Path(__file__).resolve().parent.parent.parent.parent / "data" / "jellydash.db"
)


def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    """Open a SQLite connection with WAL mode and foreign keys enabled.

    Args:
        db_path: Path to the database file. Defaults to data/jellydash.db.

    Returns:
        A configured sqlite3.Connection.
    """
    path = str(db_path or _DEFAULT_DB_PATH)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn
