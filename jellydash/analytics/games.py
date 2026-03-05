"""Jelly Games — scoring and badge logic."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass, field
from typing import Any

FILLER_PATTERN = re.compile(
    r"\b(um|uh|like|you know|basically|literally|i mean)\b", re.IGNORECASE
)

MIN_JELLIES = 3  # Minimum jellies to qualify for any game


@dataclass
class GameDef:
    """Definition of a scoring game."""

    game_id: str
    name: str
    description: str
    unit: str
    ascending: bool  # True = lower is better
    badges: dict[str, float] = field(default_factory=dict)  # tier → threshold


TRANSCRIPT_GAMES: list[GameDef] = [
    GameDef(
        game_id="filler_words",
        name="Clean Speaker",
        description=(
            "Fewest filler words per 100 words"
            " (um, uh, like, you know, basically, literally, I mean)"
        ),
        unit="fillers/100w",
        ascending=True,
        badges={"gold": 1.0, "silver": 3.0, "bronze": 5.0},
    ),
    GameDef(
        game_id="vocab_diversity",
        name="Wordsmith",
        description="Highest type-token ratio (unique words / total words)",
        unit="TTR",
        ascending=False,
        badges={"gold": 0.7, "silver": 0.55, "bronze": 0.4},
    ),
    GameDef(
        game_id="speed_talker",
        name="Speed Talker",
        description="Highest words per minute from transcript timing",
        unit="WPM",
        ascending=False,
        badges={"gold": 200.0, "silver": 160.0, "bronze": 130.0},
    ),
]

AGGREGATE_GAMES: list[GameDef] = [
    GameDef(
        game_id="engagement_magnet",
        name="Engagement Magnet",
        description="Highest likes per 100 views",
        unit="likes/100v",
        ascending=False,
        badges={"gold": 10.0, "silver": 5.0, "bronze": 2.0},
    ),
    GameDef(
        game_id="storyteller",
        name="Storyteller",
        description="Longest average jelly duration (seconds)",
        unit="sec",
        ascending=False,
        badges={"gold": 120.0, "silver": 60.0, "bronze": 30.0},
    ),
    GameDef(
        game_id="streak_master",
        name="Streak Master",
        description="Longest consecutive posting streak (days)",
        unit="days",
        ascending=False,
        badges={"gold": 14.0, "silver": 7.0, "bronze": 3.0},
    ),
]

ALL_GAMES: list[GameDef] = TRANSCRIPT_GAMES + AGGREGATE_GAMES


def score_filler_words(text: str, word_count: int) -> float | None:
    """Filler words per 100 words. Lower is better."""
    if word_count < 10:
        return None
    count = len(FILLER_PATTERN.findall(text.lower()))
    return (count / word_count) * 100


def score_vocab_diversity(text: str) -> float | None:
    """Type-token ratio. Higher is better."""
    words = text.lower().split()
    if len(words) < 10:
        return None
    return len(set(words)) / len(words)


def score_speed(word_count: int, duration: float) -> float | None:
    """Words per minute. Higher is better."""
    if duration < 5.0 or word_count < 10:
        return None
    return (word_count / duration) * 60


def _score_transcript_game(
    game_id: str, text: str, word_count: int, duration: float
) -> float | None:
    """Route to the right scorer for a transcript game."""
    if game_id == "filler_words":
        return score_filler_words(text, word_count)
    elif game_id == "vocab_diversity":
        return score_vocab_diversity(text)
    elif game_id == "speed_talker":
        return score_speed(word_count, duration)
    return None


def refresh_transcript_games(conn: sqlite3.Connection) -> int:
    """Compute transcript-based game scores for all qualified users.

    Returns:
        Number of scores written.
    """
    # Get all transcripts grouped by participant
    rows = conn.execute(
        """
        SELECT jp.participant_id, t.full_text, t.word_count, t.duration
        FROM transcripts t
        JOIN jelly_participants jp ON jp.jelly_id = t.jelly_id
        WHERE t.word_count >= 10
        """
    ).fetchall()

    # Group by participant
    user_data: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        pid = r["participant_id"]
        if pid not in user_data:
            user_data[pid] = []
        user_data[pid].append(dict(r))

    count = 0
    for game in TRANSCRIPT_GAMES:
        scores: list[tuple[str, float, int]] = []  # (pid, avg_score, sample_size)

        for pid, transcripts in user_data.items():
            if len(transcripts) < MIN_JELLIES:
                continue
            game_scores: list[float] = []
            for t in transcripts:
                s = _score_transcript_game(
                    game.game_id, t["full_text"], t["word_count"], t["duration"]
                )
                if s is not None:
                    game_scores.append(s)
            if len(game_scores) >= MIN_JELLIES:
                avg = sum(game_scores) / len(game_scores)
                scores.append((pid, avg, len(game_scores)))

        # Sort and rank
        scores.sort(key=lambda x: x[1], reverse=not game.ascending)
        conn.execute("DELETE FROM game_scores WHERE game_id = ?", (game.game_id,))
        for rank, (pid, score, sample) in enumerate(scores, 1):
            conn.execute(
                """
                INSERT INTO game_scores
                    (participant_id, game_id, score, rank, sample_size)
                VALUES (?, ?, ?, ?, ?)
                """,
                (pid, game.game_id, score, rank, sample),
            )
            count += 1

    conn.commit()
    return count


def _compute_streak(dates: list[str]) -> int:
    """Compute max consecutive posting days from sorted date strings."""
    if not dates:
        return 0
    from datetime import datetime

    parsed = sorted({datetime.fromisoformat(d).date() for d in dates if d})
    if not parsed:
        return 0

    max_streak = 1
    current = 1
    for i in range(1, len(parsed)):
        if (parsed[i] - parsed[i - 1]).days == 1:
            current += 1
            max_streak = max(max_streak, current)
        else:
            current = 1
    return max_streak


def refresh_aggregate_games(conn: sqlite3.Connection) -> int:
    """Compute aggregate-stat game scores.

    Returns:
        Number of scores written.
    """
    count = 0

    # Engagement Magnet: likes per 100 views
    rows = conn.execute(
        """
        SELECT jp.participant_id,
               SUM(j.likes_count) as total_likes,
               SUM(j.all_views) as total_views,
               COUNT(*) as n
        FROM jelly_participants jp
        JOIN jellies j ON j.id = jp.jelly_id
        GROUP BY jp.participant_id
        HAVING n >= ?
        """,
        (MIN_JELLIES,),
    ).fetchall()

    eng_scores: list[tuple[str, float, int]] = []
    for r in rows:
        if r["total_views"] > 0:
            score = (r["total_likes"] / r["total_views"]) * 100
            eng_scores.append((r["participant_id"], score, r["n"]))
    eng_scores.sort(key=lambda x: x[1], reverse=True)
    conn.execute("DELETE FROM game_scores WHERE game_id = 'engagement_magnet'")
    for rank, (pid, score, n) in enumerate(eng_scores, 1):
        conn.execute(
            """
            INSERT INTO game_scores
                (participant_id, game_id, score, rank, sample_size)
            VALUES (?, ?, ?, ?, ?)
            """,
            (pid, "engagement_magnet", score, rank, n),
        )
        count += 1

    # Storyteller: avg duration
    rows = conn.execute(
        """
        SELECT jp.participant_id, AVG(j.duration) as avg_dur, COUNT(*) as n
        FROM jelly_participants jp
        JOIN jellies j ON j.id = jp.jelly_id
        WHERE j.duration > 0
        GROUP BY jp.participant_id
        HAVING n >= ?
        """,
        (MIN_JELLIES,),
    ).fetchall()

    story_scores: list[tuple[str, float, int]] = []
    for r in rows:
        story_scores.append((r["participant_id"], r["avg_dur"], r["n"]))
    story_scores.sort(key=lambda x: x[1], reverse=True)
    conn.execute("DELETE FROM game_scores WHERE game_id = 'storyteller'")
    for rank, (pid, score, n) in enumerate(story_scores, 1):
        conn.execute(
            """
            INSERT INTO game_scores
                (participant_id, game_id, score, rank, sample_size)
            VALUES (?, ?, ?, ?, ?)
            """,
            (pid, "storyteller", score, rank, n),
        )
        count += 1

    # Streak Master: max consecutive posting days
    rows = conn.execute(
        """
        SELECT jp.participant_id, j.posted_at
        FROM jelly_participants jp
        JOIN jellies j ON j.id = jp.jelly_id
        WHERE j.posted_at IS NOT NULL
        ORDER BY jp.participant_id, j.posted_at
        """
    ).fetchall()

    user_dates: dict[str, list[str]] = {}
    for r in rows:
        pid = r["participant_id"]
        if pid not in user_dates:
            user_dates[pid] = []
        user_dates[pid].append(r["posted_at"])

    streak_scores: list[tuple[str, float, int]] = []
    for pid, dates in user_dates.items():
        if len(dates) < MIN_JELLIES:
            continue
        streak = _compute_streak(dates)
        streak_scores.append((pid, float(streak), len(dates)))
    streak_scores.sort(key=lambda x: x[1], reverse=True)
    conn.execute("DELETE FROM game_scores WHERE game_id = 'streak_master'")
    for rank, (pid, score, n) in enumerate(streak_scores, 1):
        conn.execute(
            """
            INSERT INTO game_scores
                (participant_id, game_id, score, rank, sample_size)
            VALUES (?, ?, ?, ?, ?)
            """,
            (pid, "streak_master", score, rank, n),
        )
        count += 1

    conn.commit()
    return count


def award_badges(conn: sqlite3.Connection) -> int:
    """Check game_scores against badge thresholds and award badges.

    Returns:
        Number of new badges awarded.
    """
    count = 0
    for game in ALL_GAMES:
        rows = conn.execute(
            "SELECT participant_id, score FROM game_scores WHERE game_id = ?",
            (game.game_id,),
        ).fetchall()

        for r in rows:
            for tier, threshold in game.badges.items():
                qualifies = (
                    r["score"] <= threshold
                    if game.ascending
                    else r["score"] >= threshold
                )
                if qualifies:
                    badge_id = f"{game.game_id}_{tier}"
                    try:
                        conn.execute(
                            """
                            INSERT INTO badges (participant_id, badge_id, game_id)
                            VALUES (?, ?, ?)
                            """,
                            (r["participant_id"], badge_id, game.game_id),
                        )
                        count += 1
                    except sqlite3.IntegrityError:
                        pass  # Already awarded

    conn.commit()
    return count


def refresh_all_games(conn: sqlite3.Connection) -> dict[str, int]:
    """Run all game scorers and badge awards.

    Returns:
        Dict with counts of scores and badges.
    """
    t_scores = refresh_transcript_games(conn)
    a_scores = refresh_aggregate_games(conn)
    badges = award_badges(conn)
    return {
        "transcript_scores": t_scores,
        "aggregate_scores": a_scores,
        "badges_awarded": badges,
    }
