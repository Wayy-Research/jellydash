"""Database layer for JellyDash."""

from jellydash.db.connection import get_connection
from jellydash.db.schema import create_tables

__all__ = ["get_connection", "create_tables"]
