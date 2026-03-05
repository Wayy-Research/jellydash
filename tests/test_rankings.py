"""Tests for user ranking analytics."""

from __future__ import annotations

import sqlite3

from jellyjelly.models import JellyDetail

from jellydash.analytics.rankings import get_ranked_users, refresh_user_stats
from jellydash.db.queries import upsert_jelly_detail
from tests.conftest import make_jelly_detail_dict


def _seed_users(db: sqlite3.Connection) -> None:
    """Seed DB with 3 users having different engagement."""
    for i, (views, likes) in enumerate([(1000, 50), (500, 100), (200, 10)]):
        data = make_jelly_detail_dict(
            jelly_id=f"jelly-{i}",
            user_id=f"user-{i}",
            username=f"user{i}",
            views=views,
            likes=likes,
        )
        upsert_jelly_detail(db, JellyDetail.model_validate(data))


def test_refresh_user_stats(db: sqlite3.Connection) -> None:
    """refresh_user_stats populates user_stats for all users."""
    _seed_users(db)
    count = refresh_user_stats(db)
    assert count == 3


def test_ranked_by_views(db: sqlite3.Connection) -> None:
    """Users are ranked correctly by total_views."""
    _seed_users(db)
    refresh_user_stats(db)
    ranked = get_ranked_users(db, metric="total_views")
    assert ranked[0]["username"] == "user0"
    assert ranked[0]["rank"] == 1
    assert ranked[1]["username"] == "user1"


def test_ranked_by_likes(db: sqlite3.Connection) -> None:
    """Users are ranked correctly by total_likes."""
    _seed_users(db)
    refresh_user_stats(db)
    ranked = get_ranked_users(db, metric="total_likes")
    assert ranked[0]["username"] == "user1"  # user1 has 100 likes


def test_invalid_metric(db: sqlite3.Connection) -> None:
    """Invalid metric raises ValueError."""
    import pytest

    with pytest.raises(ValueError, match="Invalid metric"):
        get_ranked_users(db, metric="hacked; DROP TABLE users")
