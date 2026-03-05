"""Shared test fixtures for JellyDash."""

from __future__ import annotations

import sqlite3
from typing import Any

import pytest

from jellydash.db.connection import get_connection
from jellydash.db.schema import create_tables


@pytest.fixture()
def db() -> sqlite3.Connection:
    """Return an in-memory SQLite database with all tables created."""
    conn = get_connection(":memory:")
    create_tables(conn)
    return conn


def make_jelly_detail_dict(
    jelly_id: str = "jelly-001",
    title: str = "Test Jelly",
    username: str = "testuser",
    user_id: str = "user-001",
    transcript_text: str = "Hello world this is a test transcript with some words",
    likes: int = 10,
    views: int = 100,
    comments: int = 5,
    tips: float = 1.0,
    duration: float = 30.0,
    posted_at: str = "2026-02-28T14:30:00Z",
) -> dict[str, Any]:
    """Build a realistic jelly detail dict for constructing JellyDetail."""
    words = []
    start = 0.0
    for w in transcript_text.split():
        words.append(
            {
                "word": w.lower(),
                "start": start,
                "end": start + 0.3,
                "confidence": 0.95,
                "punctuated_word": w,
            }
        )
        start += 0.35

    return {
        "id": jelly_id,
        "title": title,
        "started_by_id": user_id,
        "participants": [
            {
                "id": user_id,
                "username": username,
                "full_name": f"{username.title()} User",
                "pfp_url": f"https://example.com/{username}.jpg",
            }
        ],
        "posted_at": posted_at,
        "summary": f"A jelly about {title}",
        "privacy": "public",
        "thumbnail_url": "https://example.com/thumb.jpg",
        "video": {
            "original_duration": duration,
            "preview_timecode": None,
            "hls_master": "https://example.com/video.m3u8",
        },
        "transcript_overlay": {
            "results": {
                "channels": [
                    {
                        "alternatives": [
                            {
                                "words": words,
                                "transcript": transcript_text,
                            }
                        ]
                    }
                ]
            }
        },
        "likes_count": likes,
        "comments_count": comments,
        "all_views": views,
        "tips_total": tips,
    }
