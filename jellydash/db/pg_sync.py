"""Sync jellydash SQLite data to/from wayyDB PostgreSQL."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from typing import Any

logger = logging.getLogger(__name__)


def get_pg_url() -> str | None:
    """Return DATABASE_URL if configured for PostgreSQL."""
    url = os.environ.get("DATABASE_URL", "")
    if url.startswith("postgresql://") or url.startswith("postgres://"):
        return url
    return None


def _get_pg_conn() -> Any:
    """Get a psycopg2 connection to wayyDB."""
    import psycopg2
    import psycopg2.extras

    url = get_pg_url()
    if not url:
        raise RuntimeError("DATABASE_URL not set or not a PostgreSQL URL")
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


def push_to_pg(sqlite_conn: sqlite3.Connection) -> dict[str, int]:
    """Push all jellydash data from SQLite to PostgreSQL.

    Uses upserts (INSERT ... ON CONFLICT) so this is idempotent.
    Returns counts of rows synced per table.
    """
    pg = _get_pg_conn()
    cur = pg.cursor()
    counts: dict[str, int] = {}

    try:
        # Jellies
        rows = sqlite_conn.execute("SELECT * FROM jellies").fetchall()
        for r in rows:
            d = dict(r)
            cur.execute(
                """
                INSERT INTO jelly.jellies (
                    id, title, started_by_id, summary, privacy, thumbnail_url,
                    duration, hls_master, likes_count, comments_count, all_views,
                    distinct_views, anon_views, tips_total, price, pay_to_watch,
                    has_poll, has_event, posted_at, created_at, updated_at, synced_at
                ) VALUES (
                    %(id)s, %(title)s, %(started_by_id)s, %(summary)s, %(privacy)s,
                    %(thumbnail_url)s, %(duration)s, %(hls_master)s, %(likes_count)s,
                    %(comments_count)s, %(all_views)s, %(distinct_views)s, %(anon_views)s,
                    %(tips_total)s, %(price)s, %(pay_to_watch)s, %(has_poll)s,
                    %(has_event)s, %(posted_at)s, %(created_at)s, %(updated_at)s,
                    %(synced_at)s
                ) ON CONFLICT (id) DO UPDATE SET
                    title=EXCLUDED.title, summary=EXCLUDED.summary,
                    likes_count=EXCLUDED.likes_count, comments_count=EXCLUDED.comments_count,
                    all_views=EXCLUDED.all_views, distinct_views=EXCLUDED.distinct_views,
                    anon_views=EXCLUDED.anon_views, tips_total=EXCLUDED.tips_total,
                    synced_at=EXCLUDED.synced_at, updated_at=EXCLUDED.updated_at
                """,
                {
                    "id": d["id"],
                    "title": d["title"],
                    "started_by_id": d.get("started_by_id"),
                    "summary": d.get("summary"),
                    "privacy": d.get("privacy"),
                    "thumbnail_url": d.get("thumbnail_url"),
                    "duration": d.get("duration", 0.0),
                    "hls_master": d.get("hls_master"),
                    "likes_count": d.get("likes_count", 0),
                    "comments_count": d.get("comments_count", 0),
                    "all_views": d.get("all_views", 0),
                    "distinct_views": d.get("distinct_views", 0),
                    "anon_views": d.get("anon_views", 0),
                    "tips_total": d.get("tips_total", 0.0),
                    "price": d.get("price"),
                    "pay_to_watch": bool(d.get("pay_to_watch", 0)),
                    "has_poll": bool(d.get("has_poll", 0)),
                    "has_event": bool(d.get("has_event", 0)),
                    "posted_at": d.get("posted_at"),
                    "created_at": d.get("created_at"),
                    "updated_at": d.get("updated_at"),
                    "synced_at": d.get("synced_at"),
                },
            )
        counts["jellies"] = len(rows)

        # Participants
        rows = sqlite_conn.execute("SELECT * FROM participants").fetchall()
        for r in rows:
            d = dict(r)
            cur.execute(
                """
                INSERT INTO jelly.participants (id, username, full_name, pfp_url,
                    first_seen_at, last_seen_at)
                VALUES (%(id)s, %(username)s, %(full_name)s, %(pfp_url)s,
                    %(first_seen_at)s, %(last_seen_at)s)
                ON CONFLICT (id) DO UPDATE SET
                    username=EXCLUDED.username, full_name=EXCLUDED.full_name,
                    pfp_url=EXCLUDED.pfp_url, last_seen_at=EXCLUDED.last_seen_at
                """,
                d,
            )
        counts["participants"] = len(rows)

        # Jelly participants
        rows = sqlite_conn.execute("SELECT * FROM jelly_participants").fetchall()
        for r in rows:
            d = dict(r)
            cur.execute(
                """
                INSERT INTO jelly.jelly_participants (jelly_id, participant_id)
                VALUES (%(jelly_id)s, %(participant_id)s)
                ON CONFLICT DO NOTHING
                """,
                d,
            )
        counts["jelly_participants"] = len(rows)

        # Transcripts
        rows = sqlite_conn.execute("SELECT * FROM transcripts").fetchall()
        for r in rows:
            d = dict(r)
            # Convert words_json string to actual JSON for JSONB column
            words = d.get("words_json", "[]")
            if isinstance(words, str):
                try:
                    words = json.loads(words)
                except json.JSONDecodeError:
                    words = []
            cur.execute(
                """
                INSERT INTO jelly.transcripts
                    (jelly_id, full_text, words_json, word_count, duration)
                VALUES (%(jelly_id)s, %(full_text)s, %(words_json)s,
                    %(word_count)s, %(duration)s)
                ON CONFLICT (jelly_id) DO UPDATE SET
                    full_text=EXCLUDED.full_text, words_json=EXCLUDED.words_json,
                    word_count=EXCLUDED.word_count, duration=EXCLUDED.duration
                """,
                {
                    "jelly_id": d["jelly_id"],
                    "full_text": d.get("full_text", ""),
                    "words_json": json.dumps(words),
                    "word_count": d.get("word_count", 0),
                    "duration": d.get("duration", 0.0),
                },
            )
        counts["transcripts"] = len(rows)

        # Transcript segments
        rows = sqlite_conn.execute("SELECT * FROM transcript_segments").fetchall()
        for r in rows:
            d = dict(r)
            cur.execute(
                """
                INSERT INTO jelly.transcript_segments
                    (jelly_id, segment_idx, text, start_time, end_time)
                VALUES (%(jelly_id)s, %(segment_idx)s, %(text)s,
                    %(start_time)s, %(end_time)s)
                ON CONFLICT (jelly_id, segment_idx) DO UPDATE SET
                    text=EXCLUDED.text, start_time=EXCLUDED.start_time,
                    end_time=EXCLUDED.end_time
                """,
                d,
            )
        counts["transcript_segments"] = len(rows)

        # Topics
        rows = sqlite_conn.execute("SELECT * FROM topics").fetchall()
        for r in rows:
            d = dict(r)
            cur.execute(
                """
                INSERT INTO jelly.topics
                    (topic, score, period, period_start, computed_at)
                VALUES (%(topic)s, %(score)s, %(period)s,
                    %(period_start)s, %(computed_at)s)
                ON CONFLICT (topic, period, period_start) DO UPDATE SET
                    score=EXCLUDED.score, computed_at=EXCLUDED.computed_at
                """,
                d,
            )
        counts["topics"] = len(rows)

        pg.commit()
        logger.info("Pushed to PostgreSQL: %s", counts)

    except Exception:
        pg.rollback()
        raise
    finally:
        cur.close()
        pg.close()

    return counts


def pg_vector_search(
    query_text: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Full-text search on PostgreSQL using tsvector (no embeddings needed).

    Uses PostgreSQL's built-in full-text search with ranking.
    """
    pg = _get_pg_conn()
    cur = pg.cursor()

    try:
        # Use plainto_tsquery for safe user input handling
        cur.execute(
            """
            SELECT
                ts.jelly_id,
                ts.segment_idx,
                ts.text,
                ts.start_time,
                ts.end_time,
                j.title,
                j.all_views,
                j.likes_count,
                j.thumbnail_url,
                j.hls_master,
                j.duration,
                ts_rank(to_tsvector('english', ts.text),
                        plainto_tsquery('english', %s)) as rank
            FROM jelly.transcript_segments ts
            JOIN jelly.jellies j ON j.id = ts.jelly_id
            WHERE to_tsvector('english', ts.text) @@ plainto_tsquery('english', %s)
            ORDER BY rank DESC
            LIMIT %s
            """,
            (query_text, query_text, limit),
        )

        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in cur.fetchall()]
        return results

    finally:
        cur.close()
        pg.close()


def init_pg_schema() -> None:
    """Create the jelly schema on PostgreSQL if it doesn't exist."""
    import pathlib

    schema_path = pathlib.Path(__file__).parent / "pg_schema.sql"
    sql = schema_path.read_text()

    pg = _get_pg_conn()
    cur = pg.cursor()
    try:
        cur.execute(sql)
        pg.commit()
        logger.info("PostgreSQL jelly schema initialized")
    except Exception:
        pg.rollback()
        raise
    finally:
        cur.close()
        pg.close()
