"""SQLite DDL — table creation for JellyDash."""

from __future__ import annotations

import sqlite3

_TABLES: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS jellies (
        id              TEXT PRIMARY KEY,
        title           TEXT NOT NULL DEFAULT '',
        started_by_id   TEXT,
        summary         TEXT,
        privacy         TEXT,
        thumbnail_url   TEXT,
        duration        REAL NOT NULL DEFAULT 0.0,
        hls_master      TEXT,
        likes_count     INTEGER NOT NULL DEFAULT 0,
        comments_count  INTEGER NOT NULL DEFAULT 0,
        all_views       INTEGER NOT NULL DEFAULT 0,
        distinct_views  INTEGER NOT NULL DEFAULT 0,
        anon_views      INTEGER NOT NULL DEFAULT 0,
        tips_total      REAL NOT NULL DEFAULT 0.0,
        price           REAL,
        pay_to_watch    INTEGER NOT NULL DEFAULT 0,
        has_poll        INTEGER NOT NULL DEFAULT 0,
        has_event       INTEGER NOT NULL DEFAULT 0,
        posted_at       TEXT,
        created_at      TEXT,
        updated_at      TEXT,
        synced_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS participants (
        id              TEXT PRIMARY KEY,
        username        TEXT NOT NULL,
        full_name       TEXT,
        pfp_url         TEXT,
        first_seen_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        last_seen_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS jelly_participants (
        jelly_id        TEXT NOT NULL REFERENCES jellies(id),
        participant_id  TEXT NOT NULL REFERENCES participants(id),
        PRIMARY KEY (jelly_id, participant_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS transcripts (
        jelly_id        TEXT PRIMARY KEY REFERENCES jellies(id),
        full_text       TEXT NOT NULL DEFAULT '',
        words_json      TEXT NOT NULL DEFAULT '[]',
        word_count      INTEGER NOT NULL DEFAULT 0,
        duration        REAL NOT NULL DEFAULT 0.0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_stats (
        participant_id  TEXT PRIMARY KEY REFERENCES participants(id),
        total_jellies   INTEGER NOT NULL DEFAULT 0,
        total_views     INTEGER NOT NULL DEFAULT 0,
        total_likes     INTEGER NOT NULL DEFAULT 0,
        total_comments  INTEGER NOT NULL DEFAULT 0,
        total_tips      REAL NOT NULL DEFAULT 0.0,
        avg_views       REAL NOT NULL DEFAULT 0.0,
        avg_likes       REAL NOT NULL DEFAULT 0.0,
        avg_comments    REAL NOT NULL DEFAULT 0.0,
        avg_duration    REAL NOT NULL DEFAULT 0.0,
        views_per_post  REAL NOT NULL DEFAULT 0.0,
        jelly_score     REAL NOT NULL DEFAULT 0.0,
        is_rising_star  INTEGER NOT NULL DEFAULT 0,
        computed_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS game_scores (
        participant_id  TEXT NOT NULL REFERENCES participants(id),
        game_id         TEXT NOT NULL,
        score           REAL NOT NULL DEFAULT 0.0,
        rank            INTEGER NOT NULL DEFAULT 0,
        sample_size     INTEGER NOT NULL DEFAULT 0,
        computed_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        PRIMARY KEY (participant_id, game_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS badges (
        participant_id  TEXT NOT NULL REFERENCES participants(id),
        badge_id        TEXT NOT NULL,
        game_id         TEXT NOT NULL,
        awarded_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        UNIQUE (participant_id, badge_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS topics (
        topic           TEXT NOT NULL,
        score           REAL NOT NULL DEFAULT 0.0,
        period          TEXT NOT NULL,
        period_start    TEXT NOT NULL,
        computed_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        PRIMARY KEY (topic, period, period_start)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS jelly_topics (
        jelly_id        TEXT NOT NULL REFERENCES jellies(id),
        topic           TEXT NOT NULL,
        relevance       REAL NOT NULL DEFAULT 0.0,
        PRIMARY KEY (jelly_id, topic)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS transcript_segments (
        jelly_id        TEXT NOT NULL REFERENCES jellies(id),
        segment_idx     INTEGER NOT NULL,
        text            TEXT NOT NULL DEFAULT '',
        start_time      REAL NOT NULL DEFAULT 0.0,
        end_time        REAL NOT NULL DEFAULT 0.0,
        PRIMARY KEY (jelly_id, segment_idx)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sync_runs (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        finished_at     TEXT,
        status          TEXT NOT NULL DEFAULT 'running',
        jellies_found   INTEGER NOT NULL DEFAULT 0,
        jellies_detailed INTEGER NOT NULL DEFAULT 0,
        errors          INTEGER NOT NULL DEFAULT 0,
        strategy        TEXT NOT NULL DEFAULT 'full'
    )
    """,
]

_INDEXES: list[str] = [
    "CREATE INDEX IF NOT EXISTS idx_jellies_posted_at ON jellies(posted_at)",
    "CREATE INDEX IF NOT EXISTS idx_jellies_all_views ON jellies(all_views DESC)",
    "CREATE INDEX IF NOT EXISTS idx_jellies_likes ON jellies(likes_count DESC)",
    "CREATE INDEX IF NOT EXISTS idx_game_scores_game_score"
    " ON game_scores(game_id, score)",
    "CREATE INDEX IF NOT EXISTS idx_jelly_topics_topic ON jelly_topics(topic)",
    "CREATE INDEX IF NOT EXISTS idx_topics_period ON topics(period, period_start)",
    "CREATE INDEX IF NOT EXISTS idx_segments_jelly ON transcript_segments(jelly_id)",
]

_FTS_TABLES: list[str] = [
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS transcript_segments_fts
    USING fts5(
        text,
        content='transcript_segments',
        content_rowid='rowid'
    )
    """,
]

_FTS_TRIGGERS: list[str] = [
    """
    CREATE TRIGGER IF NOT EXISTS trig_segments_ai AFTER INSERT ON transcript_segments
    BEGIN
        INSERT INTO transcript_segments_fts(rowid, text)
        VALUES (new.rowid, new.text);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trig_segments_ad AFTER DELETE ON transcript_segments
    BEGIN
        INSERT INTO transcript_segments_fts(transcript_segments_fts, rowid, text)
        VALUES ('delete', old.rowid, old.text);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trig_segments_au AFTER UPDATE ON transcript_segments
    BEGIN
        INSERT INTO transcript_segments_fts(transcript_segments_fts, rowid, text)
        VALUES ('delete', old.rowid, old.text);
        INSERT INTO transcript_segments_fts(rowid, text)
        VALUES (new.rowid, new.text);
    END
    """,
]

_MIGRATIONS: list[str] = [
    "ALTER TABLE user_stats ADD COLUMN views_per_post REAL NOT NULL DEFAULT 0.0",
    "ALTER TABLE user_stats ADD COLUMN jelly_score REAL NOT NULL DEFAULT 0.0",
    "ALTER TABLE user_stats ADD COLUMN is_rising_star INTEGER NOT NULL DEFAULT 0",
]


def create_tables(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes if they don't exist."""
    cur = conn.cursor()
    for ddl in _TABLES:
        cur.execute(ddl)
    for idx in _INDEXES:
        cur.execute(idx)
    for fts_ddl in _FTS_TABLES:
        cur.execute(fts_ddl)
    for trigger in _FTS_TRIGGERS:
        cur.execute(trigger)
    # Run migrations for existing DBs (ignore if column already exists)
    for migration in _MIGRATIONS:
        try:
            cur.execute(migration)
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()
