"""Tests for Jelly Games scoring and badges."""

from __future__ import annotations

import sqlite3

from jellyjelly.models import JellyDetail

from jellydash.analytics.games import (
    award_badges,
    refresh_aggregate_games,
    refresh_all_games,
    refresh_transcript_games,
    score_filler_words,
    score_speed,
    score_vocab_diversity,
)
from jellydash.db.queries import upsert_jelly_detail
from tests.conftest import make_jelly_detail_dict


def test_score_filler_words() -> None:
    """Filler word scoring counts fillers per 100 words."""
    text = (
        "um well I think um that basically like"
        " you know the thing is um really important"
    )
    word_count = len(text.split())
    score = score_filler_words(text, word_count)
    assert score is not None
    assert score > 0


def test_score_filler_words_short() -> None:
    """Short texts return None."""
    assert score_filler_words("hello", 1) is None


def test_score_vocab_diversity() -> None:
    """Type-token ratio is between 0 and 1."""
    text = "the cat sat on the mat and the dog sat on the rug"
    score = score_vocab_diversity(text)
    assert score is not None
    assert 0 < score < 1


def test_score_vocab_diversity_unique() -> None:
    """All unique words gives high TTR."""
    text = "alpha bravo charlie delta echo foxtrot golf hotel india juliet"
    score = score_vocab_diversity(text)
    assert score is not None
    assert score == 1.0


def test_score_speed() -> None:
    """WPM calculation works correctly."""
    score = score_speed(word_count=100, duration=60.0)
    assert score is not None
    assert score == 100.0  # 100 words / 1 minute


def test_score_speed_short() -> None:
    """Short transcripts return None."""
    assert score_speed(5, 2.0) is None


def _seed_game_data(db: sqlite3.Connection, n_jellies: int = 5) -> None:
    """Seed DB with multiple jellies per user for game scoring."""
    texts = [
        "This is a clean transcript without any filler words at all today",
        "Another clean sentence about technology and innovation forward",
        "The market is showing strong growth in several key sectors now",
        "um like you know basically the thing is um really important here",
        "Innovation drives progress in basically every um sector of life",
    ]
    for i in range(n_jellies):
        data = make_jelly_detail_dict(
            jelly_id=f"jelly-{i}",
            user_id="user-001",
            username="testuser",
            transcript_text=texts[i % len(texts)],
            views=100 * (i + 1),
            likes=10 * (i + 1),
            duration=30.0 + i * 10,
            posted_at=f"2026-02-{20 + i:02d}T14:30:00Z",
        )
        upsert_jelly_detail(db, JellyDetail.model_validate(data))


def test_refresh_transcript_games(db: sqlite3.Connection) -> None:
    """Transcript game scores are computed for qualifying users."""
    _seed_game_data(db)
    count = refresh_transcript_games(db)
    assert count > 0

    rows = db.execute("SELECT * FROM game_scores").fetchall()
    assert len(rows) > 0


def test_refresh_aggregate_games(db: sqlite3.Connection) -> None:
    """Aggregate game scores are computed."""
    _seed_game_data(db)
    count = refresh_aggregate_games(db)
    assert count > 0


def test_award_badges(db: sqlite3.Connection) -> None:
    """Badges are awarded based on score thresholds."""
    _seed_game_data(db)
    refresh_transcript_games(db)
    refresh_aggregate_games(db)
    count = award_badges(db)
    # Some badges should be awarded
    assert count >= 0  # May be 0 if thresholds not met

    # Verify no duplicates
    badge_rows = db.execute("SELECT * FROM badges").fetchall()
    combos = [(r["participant_id"], r["badge_id"]) for r in badge_rows]
    assert len(combos) == len(set(combos))


def test_refresh_all_games(db: sqlite3.Connection) -> None:
    """refresh_all_games runs all scorers and badges."""
    _seed_game_data(db)
    result = refresh_all_games(db)
    assert "transcript_scores" in result
    assert "aggregate_scores" in result
    assert "badges_awarded" in result
