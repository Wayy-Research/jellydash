"""Groq-powered topic extraction from transcripts.

Processes transcripts in batches via Groq LLM to extract meaningful,
complex topics. Tracks which jellies have been processed to enable
incremental extraction and stay within free-tier API limits.

Groq free tier: ~30 req/min, ~14,400 req/day.
Strategy: batch 10 transcripts per call, process incrementally.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"

# How many transcripts to send per Groq call
BATCH_SIZE = 10
# Max batches per refresh call (to stay under rate limits)
MAX_BATCHES_PER_RUN = 5
# Truncate transcripts to this many chars to fit in context
TRANSCRIPT_MAX_CHARS = 1500

_EXTRACT_SYSTEM = """\
You are a topic extraction engine for a video platform. Given a batch of \
video transcripts, extract the specific, meaningful topics being discussed.

Rules:
- Return 3-5 topics PER transcript (not generic labels)
- Topics should be specific enough to be searchable (e.g. "bitcoin ETF approval" not "finance")
- Prefer noun phrases: "AI code generation", "startup fundraising strategy", "SEC crypto regulation"
- Each topic should be 2-5 words
- Score each topic 0.0-1.0 by how central it is to that transcript
- Return valid JSON only, no markdown

Output format:
{
  "results": [
    {
      "id": "<jelly_id>",
      "topics": [
        {"topic": "bitcoin ETF approval", "score": 0.9},
        {"topic": "institutional crypto adoption", "score": 0.7}
      ]
    }
  ],
  "trending": [
    {"topic": "bitcoin ETF approval", "score": 0.95},
    {"topic": "AI code generation", "score": 0.8}
  ]
}

The "trending" array should contain the 5-10 most significant cross-cutting \
topics across ALL transcripts in this batch, scored by prominence."""

_TRENDING_SYSTEM = """\
You are a trend analysis engine. Given a list of topics extracted from recent \
video transcripts on a social platform, identify the top trending themes.

Rules:
- Group similar topics into broader trends
- Rank by frequency and recency
- Return specific, meaningful trend names (not generic)
- Score 0.0-1.0 by prominence

Return valid JSON only:
{
  "trends": [
    {"topic": "AI regulation debate", "score": 0.95, "related": ["SEC AI rules", "EU AI act"]},
    {"topic": "bitcoin price rally", "score": 0.8, "related": ["crypto ETF", "institutional buying"]}
  ]
}"""


def _get_api_key() -> str | None:
    return os.environ.get("GROQ_API_KEY")


def _groq_call(system: str, user: str) -> dict[str, Any] | None:
    """Make a single Groq API call. Returns parsed JSON or None."""
    api_key = _get_api_key()
    if not api_key:
        return None

    try:
        resp = httpx.post(
            GROQ_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": 0.1,
                "max_tokens": 2000,
                "response_format": {"type": "json_object"},
            },
            timeout=30.0,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"].strip()
        return json.loads(content)  # type: ignore[no-any-return]
    except Exception:
        logger.exception("Groq topic extraction call failed")
        return None


def _get_unprocessed_jellies(
    conn: sqlite3.Connection, limit: int = 50
) -> list[dict[str, Any]]:
    """Get jellies with transcripts that haven't been topic-extracted yet."""
    rows = conn.execute(
        """
        SELECT t.jelly_id, t.full_text, j.title, j.posted_at
        FROM transcripts t
        JOIN jellies j ON j.id = t.jelly_id
        WHERE t.word_count > 20
        AND t.jelly_id NOT IN (SELECT DISTINCT jelly_id FROM jelly_topics)
        ORDER BY j.posted_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def extract_batch_topics(
    conn: sqlite3.Connection,
    batch: list[dict[str, Any]],
) -> tuple[int, list[tuple[str, float]]]:
    """Extract topics for a batch of jellies via Groq.

    Returns (jelly_topics_stored, trending_topics).
    """
    # Build prompt
    entries = []
    for j in batch:
        text = j["full_text"][:TRANSCRIPT_MAX_CHARS]
        entries.append(f'[ID: {j["jelly_id"]}] Title: {j["title"]}\n{text}')

    user_msg = "Extract topics from these transcripts:\n\n" + "\n\n---\n\n".join(entries)
    result = _groq_call(_EXTRACT_SYSTEM, user_msg)

    if not result:
        return 0, []

    stored = 0
    trending: list[tuple[str, float]] = []

    # Store per-jelly topics
    for item in result.get("results", []):
        jid = item.get("id", "")
        for t in item.get("topics", []):
            topic = t.get("topic", "").strip().lower()
            score = float(t.get("score", 0.0))
            if topic and score > 0:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO jelly_topics (jelly_id, topic, relevance)
                    VALUES (?, ?, ?)
                    """,
                    (jid, topic, score),
                )
                stored += 1

    # Collect trending from this batch
    for t in result.get("trending", []):
        topic = t.get("topic", "").strip().lower()
        score = float(t.get("score", 0.0))
        if topic and score > 0:
            trending.append((topic, score))

    conn.commit()
    return stored, trending


def extract_topics_incremental(
    conn: sqlite3.Connection,
    max_batches: int = MAX_BATCHES_PER_RUN,
    batch_size: int = BATCH_SIZE,
) -> dict[str, Any]:
    """Process unextracted jellies in batches. Call on sync or on-demand.

    Returns stats about what was processed.
    """
    if not _get_api_key():
        return {"error": "GROQ_API_KEY not set", "processed": 0}

    unprocessed = _get_unprocessed_jellies(conn, limit=max_batches * batch_size)
    if not unprocessed:
        return {"processed": 0, "remaining": 0, "message": "All jellies processed"}

    total_stored = 0
    all_trending: list[tuple[str, float]] = []
    batches_run = 0

    for i in range(0, len(unprocessed), batch_size):
        if batches_run >= max_batches:
            break
        batch = unprocessed[i : i + batch_size]
        stored, trending = extract_batch_topics(conn, batch)
        total_stored += stored
        all_trending.extend(trending)
        batches_run += 1
        logger.info(
            "Topic batch %d: %d topics from %d jellies",
            batches_run, stored, len(batch),
        )

    remaining = len(unprocessed) - (batches_run * batch_size)

    return {
        "processed": batches_run * batch_size,
        "topics_stored": total_stored,
        "trending_found": len(all_trending),
        "remaining": max(0, remaining),
        "batches": batches_run,
    }


def _period_start(period: str) -> str:
    """Get ISO start date for a named period."""
    now = datetime.now(timezone.utc)
    if period == "24h":
        dt = now - timedelta(hours=24)
    elif period == "7d":
        dt = now - timedelta(days=7)
    elif period == "30d":
        dt = now - timedelta(days=30)
    else:
        return "2020-01-01T00:00:00Z"
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def refresh_topics(
    conn: sqlite3.Connection,
    period: str = "all_time",
    top_n: int = 30,
) -> int:
    """Recompute trending topics for a period.

    If Groq is available: aggregates per-jelly topics from jelly_topics,
    then uses Groq to synthesize top trends.

    If Groq is unavailable: aggregates per-jelly topics by frequency.
    """
    ps = _period_start(period)

    # First, run incremental extraction if Groq is available
    if _get_api_key():
        extract_topics_incremental(conn, max_batches=2)

    # Aggregate jelly_topics for the period
    if period == "all_time":
        rows = conn.execute(
            """
            SELECT jt.topic, AVG(jt.relevance) as avg_rel,
                   COUNT(*) as freq
            FROM jelly_topics jt
            GROUP BY jt.topic
            HAVING freq >= 2
            ORDER BY freq * avg_rel DESC
            LIMIT 100
            """,
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT jt.topic, AVG(jt.relevance) as avg_rel,
                   COUNT(*) as freq
            FROM jelly_topics jt
            JOIN jellies j ON j.id = jt.jelly_id
            WHERE j.posted_at >= ?
            GROUP BY jt.topic
            HAVING freq >= 1
            ORDER BY freq * avg_rel DESC
            LIMIT 100
            """,
            (ps,),
        ).fetchall()

    raw_topics = [(r["topic"], r["avg_rel"], r["freq"]) for r in rows]

    if not raw_topics:
        return 0

    # Try Groq synthesis for richer trending analysis
    final_topics: list[tuple[str, float]] = []

    if _get_api_key() and len(raw_topics) >= 5:
        topic_list = "\n".join(
            f"- {t} (appears {f}x, avg relevance {s:.2f})"
            for t, s, f in raw_topics[:60]
        )
        user_msg = (
            f"These are topics extracted from videos in the last "
            f"{'24 hours' if period == '24h' else period}:\n\n"
            f"{topic_list}\n\n"
            f"Identify the top {top_n} trending themes."
        )
        result = _groq_call(_TRENDING_SYSTEM, user_msg)

        if result and "trends" in result:
            for t in result["trends"][:top_n]:
                topic = t.get("topic", "").strip().lower()
                score = float(t.get("score", 0.0))
                if topic and score > 0:
                    final_topics.append((topic, score))

    # Fallback: use raw aggregated topics if Groq didn't produce results
    if not final_topics:
        for topic, avg_rel, freq in raw_topics[:top_n]:
            # Score = freq-weighted relevance, normalized
            score = min(1.0, (freq * avg_rel) / 5.0)
            final_topics.append((topic, score))

    # Store
    conn.execute(
        "DELETE FROM topics WHERE period = ? AND period_start = ?", (period, ps)
    )
    for term, score in final_topics:
        conn.execute(
            """
            INSERT INTO topics (topic, score, period, period_start)
            VALUES (?, ?, ?, ?)
            """,
            (term, score, period, ps),
        )

    conn.commit()
    return len(final_topics)
