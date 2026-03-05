"""Typed query functions for JellyDash SQLite database."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

from jellyjelly.models import JellyDetail, TranscriptWord


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def upsert_jelly_detail(conn: sqlite3.Connection, detail: JellyDetail) -> None:
    """Insert or update a jelly and its related data."""
    now = _now_iso()
    cur = conn.cursor()

    # Upsert jelly
    cur.execute(
        """
        INSERT INTO jellies (
            id, title, started_by_id, summary, privacy, thumbnail_url,
            duration, hls_master, likes_count, comments_count, all_views,
            distinct_views, anon_views, tips_total, price, pay_to_watch,
            has_poll, has_event, posted_at, created_at, updated_at, synced_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            title=excluded.title, summary=excluded.summary,
            likes_count=excluded.likes_count, comments_count=excluded.comments_count,
            all_views=excluded.all_views, distinct_views=excluded.distinct_views,
            anon_views=excluded.anon_views, tips_total=excluded.tips_total,
            synced_at=excluded.synced_at, updated_at=excluded.updated_at
        """,
        (
            detail.id,
            detail.title,
            detail.started_by_id,
            detail.summary,
            detail.privacy,
            detail.thumbnail_url,
            detail.duration_seconds,
            detail.video.hls_master if detail.video else None,
            detail.likes_count,
            detail.comments_count,
            detail.all_views,
            detail.distinct_views or 0,
            detail.anon_views or 0,
            detail.tips_total,
            detail.price,
            int(detail.pay_to_watch or False),
            int(detail.has_poll or False),
            int(detail.has_event or False),
            detail.posted_at.isoformat() if detail.posted_at else None,
            detail.created_at.isoformat() if detail.created_at else None,
            detail.updated_at.isoformat() if detail.updated_at else None,
            now,
        ),
    )

    # Upsert participants + join table
    for p in detail.participants:
        cur.execute(
            """
            INSERT INTO participants (id, username, full_name, pfp_url, last_seen_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                username=excluded.username, full_name=excluded.full_name,
                pfp_url=excluded.pfp_url, last_seen_at=excluded.last_seen_at
            """,
            (p.id, p.username, p.full_name, p.pfp_url, now),
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO jelly_participants (jelly_id, participant_id)
            VALUES (?, ?)
            """,
            (detail.id, p.id),
        )

    # Upsert transcript
    transcript_text = detail.transcript_text
    words: list[dict[str, Any]] = []
    transcript_duration = 0.0
    if detail.transcript_overlay and detail.transcript_overlay.results:
        for ch in detail.transcript_overlay.results.channels:
            for alt in ch.alternatives:
                for w in alt.words:
                    words.append(
                        {
                            "word": w.word,
                            "start": w.start,
                            "end": w.end,
                            "confidence": w.confidence,
                        }
                    )
                    if w.end > transcript_duration:
                        transcript_duration = w.end

    word_count = len(transcript_text.split()) if transcript_text else 0
    cur.execute(
        """
        INSERT INTO transcripts (jelly_id, full_text, words_json, word_count, duration)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(jelly_id) DO UPDATE SET
            full_text=excluded.full_text, words_json=excluded.words_json,
            word_count=excluded.word_count, duration=excluded.duration
        """,
        (
            detail.id,
            transcript_text,
            json.dumps(words),
            word_count,
            transcript_duration,
        ),
    )
    conn.commit()


def get_existing_ids(conn: sqlite3.Connection) -> set[str]:
    """Return all jelly IDs currently in the database."""
    rows = conn.execute("SELECT id FROM jellies").fetchall()
    return {row["id"] for row in rows}


def get_stale_ids(conn: sqlite3.Connection, max_age_hours: int = 24) -> list[str]:
    """Return jelly IDs whose synced_at is older than max_age_hours."""
    rows = conn.execute(
        """
        SELECT id FROM jellies
        WHERE synced_at < strftime('%Y-%m-%dT%H:%M:%SZ',
              datetime('now', ? || ' hours'))
        """,
        (f"-{max_age_hours}",),
    ).fetchall()
    return [row["id"] for row in rows]


def get_jelly_by_id(conn: sqlite3.Connection, jelly_id: str) -> dict[str, Any] | None:
    """Fetch a single jelly row as a dict."""
    row = conn.execute("SELECT * FROM jellies WHERE id = ?", (jelly_id,)).fetchone()
    return dict(row) if row else None


def get_participant_by_id(
    conn: sqlite3.Connection, participant_id: str
) -> dict[str, Any] | None:
    """Fetch a single participant row as a dict."""
    row = conn.execute(
        "SELECT * FROM participants WHERE id = ?", (participant_id,)
    ).fetchone()
    return dict(row) if row else None


def get_transcript(conn: sqlite3.Connection, jelly_id: str) -> dict[str, Any] | None:
    """Fetch transcript for a jelly."""
    row = conn.execute(
        "SELECT * FROM transcripts WHERE jelly_id = ?", (jelly_id,)
    ).fetchone()
    return dict(row) if row else None


def get_transcript_words(
    conn: sqlite3.Connection, jelly_id: str
) -> list[TranscriptWord]:
    """Return parsed TranscriptWord list for a jelly."""
    row = conn.execute(
        "SELECT words_json FROM transcripts WHERE jelly_id = ?", (jelly_id,)
    ).fetchone()
    if not row:
        return []
    raw: list[dict[str, Any]] = json.loads(row["words_json"])
    return [TranscriptWord(**w) for w in raw]


def search_transcripts(conn: sqlite3.Connection, query: str) -> list[dict[str, Any]]:
    """Search transcripts by full_text LIKE match."""
    rows = conn.execute(
        """
        SELECT t.jelly_id, t.full_text, j.title, j.all_views, j.likes_count
        FROM transcripts t
        JOIN jellies j ON j.id = t.jelly_id
        WHERE t.full_text LIKE ?
        ORDER BY j.all_views DESC
        LIMIT 50
        """,
        (f"%{query}%",),
    ).fetchall()
    return [dict(r) for r in rows]


def get_all_user_stats(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Fetch all user stats joined with participant info."""
    rows = conn.execute(
        """
        SELECT us.*, p.username, p.full_name, p.pfp_url
        FROM user_stats us
        JOIN participants p ON p.id = us.participant_id
        ORDER BY us.total_views DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def get_game_leaderboard(
    conn: sqlite3.Connection, game_id: str, limit: int = 20
) -> list[dict[str, Any]]:
    """Fetch top scores for a game."""
    rows = conn.execute(
        """
        SELECT gs.*, p.username, p.full_name, p.pfp_url
        FROM game_scores gs
        JOIN participants p ON p.id = gs.participant_id
        WHERE gs.game_id = ?
        ORDER BY gs.rank ASC
        LIMIT ?
        """,
        (game_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_user_badges(
    conn: sqlite3.Connection, participant_id: str
) -> list[dict[str, Any]]:
    """Fetch all badges for a user."""
    rows = conn.execute(
        "SELECT * FROM badges WHERE participant_id = ?", (participant_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_topics(
    conn: sqlite3.Connection, period: str, limit: int = 30
) -> list[dict[str, Any]]:
    """Fetch top topics for a period, ordered by most recent period_start."""
    rows = conn.execute(
        """
        SELECT * FROM topics
        WHERE period = ?
        ORDER BY period_start DESC, score DESC
        LIMIT ?
        """,
        (period, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_sync_runs(conn: sqlite3.Connection, limit: int = 20) -> list[dict[str, Any]]:
    """Fetch recent sync run history."""
    rows = conn.execute(
        "SELECT * FROM sync_runs ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def start_sync_run(conn: sqlite3.Connection, strategy: str = "full") -> int:
    """Insert a new sync run and return its ID."""
    cur = conn.execute("INSERT INTO sync_runs (strategy) VALUES (?)", (strategy,))
    conn.commit()
    return cur.lastrowid or 0


def finish_sync_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str = "completed",
    jellies_found: int = 0,
    jellies_detailed: int = 0,
    errors: int = 0,
) -> None:
    """Mark a sync run as finished."""
    conn.execute(
        """
        UPDATE sync_runs
        SET finished_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
            status = ?, jellies_found = ?, jellies_detailed = ?, errors = ?
        WHERE id = ?
        """,
        (status, jellies_found, jellies_detailed, errors, run_id),
    )
    conn.commit()


def get_platform_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    """Get high-level platform stats for the home page."""
    row = conn.execute(
        """
        SELECT
            COUNT(*) as total_jellies,
            COALESCE(SUM(all_views), 0) as total_views,
            COALESCE(SUM(likes_count), 0) as total_likes,
            COALESCE(SUM(comments_count), 0) as total_comments,
            COALESCE(SUM(tips_total), 0) as total_tips
        FROM jellies
        """
    ).fetchone()
    stats: dict[str, Any] = dict(row) if row else {}

    user_row = conn.execute(
        "SELECT COUNT(*) as total_users FROM participants"
    ).fetchone()
    stats["total_users"] = user_row["total_users"] if user_row else 0

    sync_row = conn.execute(
        "SELECT finished_at FROM sync_runs"
        " WHERE status='completed' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    stats["last_sync"] = sync_row["finished_at"] if sync_row else None

    return stats


def get_jellies_per_day(
    conn: sqlite3.Connection, limit: int = 90
) -> list[dict[str, Any]]:
    """Get jelly counts per day for time series chart."""
    rows = conn.execute(
        """
        SELECT DATE(posted_at) as day, COUNT(*) as count
        FROM jellies
        WHERE posted_at IS NOT NULL
        GROUP BY DATE(posted_at)
        ORDER BY day DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_user_jellies(
    conn: sqlite3.Connection, participant_id: str, limit: int = 50
) -> list[dict[str, Any]]:
    """Get jellies for a specific user."""
    rows = conn.execute(
        """
        SELECT j.*
        FROM jellies j
        JOIN jelly_participants jp ON jp.jelly_id = j.id
        WHERE jp.participant_id = ?
        ORDER BY j.posted_at DESC
        LIMIT ?
        """,
        (participant_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]
