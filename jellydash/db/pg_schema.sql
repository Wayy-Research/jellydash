-- JellyDash PostgreSQL schema for wayyDB
-- Host: 100.96.150.42:5432 (Tailscale) | Database: wayydb
-- Uses: pgvector for transcript segment embeddings

CREATE SCHEMA IF NOT EXISTS jelly;

-- ── Core tables ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS jelly.jellies (
    id              TEXT PRIMARY KEY,
    title           TEXT NOT NULL DEFAULT '',
    started_by_id   TEXT,
    summary         TEXT,
    privacy         TEXT,
    thumbnail_url   TEXT,
    duration        DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    hls_master      TEXT,
    likes_count     INTEGER NOT NULL DEFAULT 0,
    comments_count  INTEGER NOT NULL DEFAULT 0,
    all_views       INTEGER NOT NULL DEFAULT 0,
    distinct_views  INTEGER NOT NULL DEFAULT 0,
    anon_views      INTEGER NOT NULL DEFAULT 0,
    tips_total      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    price           DOUBLE PRECISION,
    pay_to_watch    BOOLEAN NOT NULL DEFAULT false,
    has_poll        BOOLEAN NOT NULL DEFAULT false,
    has_event       BOOLEAN NOT NULL DEFAULT false,
    posted_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jellies_posted_at ON jelly.jellies(posted_at);
CREATE INDEX IF NOT EXISTS idx_jellies_all_views ON jelly.jellies(all_views DESC);
CREATE INDEX IF NOT EXISTS idx_jellies_likes ON jelly.jellies(likes_count DESC);

CREATE TABLE IF NOT EXISTS jelly.participants (
    id              TEXT PRIMARY KEY,
    username        TEXT NOT NULL,
    full_name       TEXT,
    pfp_url         TEXT,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jelly.jelly_participants (
    jelly_id        TEXT NOT NULL REFERENCES jelly.jellies(id),
    participant_id  TEXT NOT NULL REFERENCES jelly.participants(id),
    PRIMARY KEY (jelly_id, participant_id)
);

CREATE TABLE IF NOT EXISTS jelly.transcripts (
    jelly_id        TEXT PRIMARY KEY REFERENCES jelly.jellies(id),
    full_text       TEXT NOT NULL DEFAULT '',
    words_json      JSONB NOT NULL DEFAULT '[]',
    word_count      INTEGER NOT NULL DEFAULT 0,
    duration        DOUBLE PRECISION NOT NULL DEFAULT 0.0
);

-- ── Analytics tables ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS jelly.user_stats (
    participant_id  TEXT PRIMARY KEY REFERENCES jelly.participants(id),
    total_jellies   INTEGER NOT NULL DEFAULT 0,
    total_views     INTEGER NOT NULL DEFAULT 0,
    total_likes     INTEGER NOT NULL DEFAULT 0,
    total_comments  INTEGER NOT NULL DEFAULT 0,
    total_tips      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    avg_views       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    avg_likes       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    avg_comments    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    avg_duration    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    views_per_post  DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    jelly_score     DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    is_rising_star  BOOLEAN NOT NULL DEFAULT false,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jelly.game_scores (
    participant_id  TEXT NOT NULL REFERENCES jelly.participants(id),
    game_id         TEXT NOT NULL,
    score           DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    rank            INTEGER NOT NULL DEFAULT 0,
    sample_size     INTEGER NOT NULL DEFAULT 0,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (participant_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_game_scores_game
    ON jelly.game_scores(game_id, score);

CREATE TABLE IF NOT EXISTS jelly.badges (
    participant_id  TEXT NOT NULL REFERENCES jelly.participants(id),
    badge_id        TEXT NOT NULL,
    game_id         TEXT NOT NULL,
    awarded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (participant_id, badge_id)
);

CREATE TABLE IF NOT EXISTS jelly.topics (
    topic           TEXT NOT NULL,
    score           DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    period          TEXT NOT NULL,
    period_start    TIMESTAMPTZ NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, period, period_start)
);

CREATE INDEX IF NOT EXISTS idx_topics_period
    ON jelly.topics(period, period_start);

CREATE TABLE IF NOT EXISTS jelly.jelly_topics (
    jelly_id        TEXT NOT NULL REFERENCES jelly.jellies(id),
    topic           TEXT NOT NULL,
    relevance       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    PRIMARY KEY (jelly_id, topic)
);

CREATE INDEX IF NOT EXISTS idx_jelly_topics_topic ON jelly.jelly_topics(topic);

CREATE TABLE IF NOT EXISTS jelly.sync_runs (
    id              SERIAL PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running',
    jellies_found   INTEGER NOT NULL DEFAULT 0,
    jellies_detailed INTEGER NOT NULL DEFAULT 0,
    errors          INTEGER NOT NULL DEFAULT 0,
    strategy        TEXT NOT NULL DEFAULT 'full'
);

-- ── Context Layer: transcript segments + pgvector embeddings ─────────────────

CREATE TABLE IF NOT EXISTS jelly.transcript_segments (
    jelly_id        TEXT NOT NULL REFERENCES jelly.jellies(id),
    segment_idx     INTEGER NOT NULL,
    text            TEXT NOT NULL DEFAULT '',
    start_time      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    end_time        DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    embedding       vector(1024),  -- Groq/llama embeddings (1024-dim)
    PRIMARY KEY (jelly_id, segment_idx)
);

CREATE INDEX IF NOT EXISTS idx_segments_jelly
    ON jelly.transcript_segments(jelly_id);

-- IVFFlat index for fast cosine similarity search
CREATE INDEX IF NOT EXISTS idx_segments_embedding
    ON jelly.transcript_segments
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- Full-text search index on segment text
CREATE INDEX IF NOT EXISTS idx_segments_text_gin
    ON jelly.transcript_segments
    USING gin (to_tsvector('english', text));

-- ── Permissions ──────────────────────────────────────────────────────────────

GRANT USAGE ON SCHEMA jelly TO wayy;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA jelly TO wayy;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA jelly TO wayy;
