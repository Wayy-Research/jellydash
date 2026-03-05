"""Tests for the database layer."""

from __future__ import annotations

import sqlite3

from jellyjelly.models import JellyDetail

from jellydash.db.queries import (
    get_existing_ids,
    get_jelly_by_id,
    get_participant_by_id,
    get_platform_stats,
    get_transcript,
    upsert_jelly_detail,
)
from tests.conftest import make_jelly_detail_dict


def test_create_tables(db: sqlite3.Connection) -> None:
    """All expected tables exist after create_tables."""
    tables = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = {r["name"] for r in tables}
    expected = {
        "jellies",
        "participants",
        "jelly_participants",
        "transcripts",
        "user_stats",
        "game_scores",
        "badges",
        "topics",
        "jelly_topics",
        "sync_runs",
    }
    assert expected.issubset(names)


def test_upsert_jelly_detail(db: sqlite3.Connection) -> None:
    """Upserting a jelly stores jelly, participant, and transcript."""
    data = make_jelly_detail_dict()
    detail = JellyDetail.model_validate(data)
    upsert_jelly_detail(db, detail)

    jelly = get_jelly_by_id(db, "jelly-001")
    assert jelly is not None
    assert jelly["title"] == "Test Jelly"
    assert jelly["likes_count"] == 10
    assert jelly["all_views"] == 100

    participant = get_participant_by_id(db, "user-001")
    assert participant is not None
    assert participant["username"] == "testuser"

    transcript = get_transcript(db, "jelly-001")
    assert transcript is not None
    assert "Hello world" in transcript["full_text"]
    assert transcript["word_count"] > 0


def test_upsert_updates_existing(db: sqlite3.Connection) -> None:
    """Upserting the same jelly updates metrics."""
    data = make_jelly_detail_dict(likes=10, views=100)
    detail = JellyDetail.model_validate(data)
    upsert_jelly_detail(db, detail)

    data2 = make_jelly_detail_dict(likes=20, views=200)
    detail2 = JellyDetail.model_validate(data2)
    upsert_jelly_detail(db, detail2)

    jelly = get_jelly_by_id(db, "jelly-001")
    assert jelly is not None
    assert jelly["likes_count"] == 20
    assert jelly["all_views"] == 200


def test_get_existing_ids(db: sqlite3.Connection) -> None:
    """get_existing_ids returns all stored jelly IDs."""
    for i in range(3):
        data = make_jelly_detail_dict(jelly_id=f"jelly-{i}", user_id=f"user-{i}")
        detail = JellyDetail.model_validate(data)
        upsert_jelly_detail(db, detail)

    ids = get_existing_ids(db)
    assert ids == {"jelly-0", "jelly-1", "jelly-2"}


def test_platform_stats(db: sqlite3.Connection) -> None:
    """Platform stats aggregate correctly."""
    for i in range(3):
        data = make_jelly_detail_dict(
            jelly_id=f"jelly-{i}",
            user_id=f"user-{i}",
            username=f"user{i}",
            views=100,
            likes=10,
        )
        detail = JellyDetail.model_validate(data)
        upsert_jelly_detail(db, detail)

    stats = get_platform_stats(db)
    assert stats["total_jellies"] == 3
    assert stats["total_views"] == 300
    assert stats["total_likes"] == 30
    assert stats["total_users"] == 3
