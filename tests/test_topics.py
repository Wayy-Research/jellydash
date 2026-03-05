"""Tests for topic extraction analytics."""

from __future__ import annotations

import sqlite3

from jellyjelly.models import JellyDetail

from jellydash.analytics.topics import extract_topics, refresh_topics
from jellydash.db.queries import upsert_jelly_detail
from tests.conftest import make_jelly_detail_dict


def test_extract_topics_basic() -> None:
    """extract_topics returns scored terms."""
    texts = [
        "Machine learning and artificial intelligence are transforming finance",
        "Deep learning models for financial prediction and risk assessment",
        "Neural networks applied to stock market forecasting with machine learning",
        "Artificial intelligence powered trading systems and portfolio management",
    ]
    topics = extract_topics(texts, top_n=5)
    assert len(topics) > 0
    assert all(isinstance(t, tuple) and len(t) == 2 for t in topics)
    assert all(score > 0 for _, score in topics)


def test_extract_topics_empty() -> None:
    """extract_topics returns empty for insufficient texts."""
    assert extract_topics([]) == []
    assert extract_topics(["single text"]) == []


def test_refresh_topics(db: sqlite3.Connection) -> None:
    """refresh_topics stores topics in DB."""
    texts = [
        "Bitcoin cryptocurrency trading blockchain technology",
        "Ethereum smart contracts decentralized finance blockchain",
        "Cryptocurrency market analysis bitcoin price prediction",
        "Blockchain technology revolutionizing cryptocurrency trading",
    ]
    for i, text in enumerate(texts):
        data = make_jelly_detail_dict(
            jelly_id=f"jelly-{i}",
            user_id=f"user-{i}",
            username=f"user{i}",
            transcript_text=text,
        )
        upsert_jelly_detail(db, JellyDetail.model_validate(data))

    count = refresh_topics(db, period="all_time", top_n=10)
    assert count > 0

    rows = db.execute("SELECT * FROM topics WHERE period = 'all_time'").fetchall()
    assert len(rows) > 0
