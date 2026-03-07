"""Groq-powered semantic search over transcript segments."""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

import httpx


GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"


def _get_api_key() -> str | None:
    return os.environ.get("GROQ_API_KEY")


def _sanitize_fts_query(query: str) -> str:
    """Escape user input for FTS5 literal matching."""
    tokens = query.strip().split()
    return " ".join(f'"{t}"' for t in tokens if t)


def _fts_search(
    conn: sqlite3.Connection, query: str, limit: int = 50
) -> list[dict[str, Any]]:
    """Full-text search across transcript segments using FTS5."""
    fts_query = _sanitize_fts_query(query)
    if not fts_query:
        return []
    rows = conn.execute(
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
            j.duration
        FROM transcript_segments_fts fts
        JOIN transcript_segments ts ON ts.rowid = fts.rowid
        JOIN jellies j ON j.id = ts.jelly_id
        WHERE transcript_segments_fts MATCH ?
        ORDER BY rank
        LIMIT ?
        """,
        (fts_query, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def _like_search(
    conn: sqlite3.Connection, query: str, limit: int = 50
) -> list[dict[str, Any]]:
    """Fallback LIKE search when FTS query syntax fails."""
    escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    rows = conn.execute(
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
            j.duration
        FROM transcript_segments ts
        JOIN jellies j ON j.id = ts.jelly_id
        WHERE ts.text LIKE ? ESCAPE '\\'
        ORDER BY j.all_views DESC
        LIMIT ?
        """,
        (f"%{escaped}%", limit),
    ).fetchall()
    return [dict(r) for r in rows]


def text_search(
    conn: sqlite3.Connection, query: str, limit: int = 50
) -> list[dict[str, Any]]:
    """Search transcript segments, trying FTS5 first with LIKE fallback."""
    try:
        results = _fts_search(conn, query, limit)
    except Exception:
        results = _like_search(conn, query, limit)
    return results


def rerank_with_groq(
    query: str,
    results: list[dict[str, Any]],
    top_k: int = 10,
) -> list[dict[str, Any]]:
    """Use Groq LLM to rerank search results by semantic relevance.

    Falls back to original results if Groq is unavailable.
    """
    api_key = _get_api_key()
    if not api_key or not results:
        return results[:top_k]

    # Build candidate list for the LLM
    candidates = []
    for i, r in enumerate(results[:30]):  # Send max 30 to rerank
        candidates.append(
            f"[{i}] ({r['start_time']:.0f}s-{r['end_time']:.0f}s) "
            f"{r['title']}: {r['text'][:200]}"
        )

    safe_query = query[:200].strip()
    prompt = (
        "You are a search reranker. You MUST respond with ONLY a JSON array of "
        "integers. No other text. Given the search query and candidates, return "
        f"the indices of the top {top_k} most relevant results.\n\n"
        f"Query: {safe_query}\n\n"
        "Candidates:\n" + "\n".join(candidates)
    )

    try:
        resp = httpx.post(
            GROQ_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": GROQ_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0,
                "max_tokens": 200,
            },
            timeout=10.0,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"].strip()

        # Parse the JSON array of indices
        # Handle cases where LLM wraps in markdown code blocks
        if content.startswith("```"):
            content = content.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        indices = json.loads(content)
        if isinstance(indices, list):
            reranked = []
            seen: set[int] = set()
            for idx in indices:
                if isinstance(idx, int) and 0 <= idx < len(results) and idx not in seen:
                    reranked.append(results[idx])
                    seen.add(idx)
            if reranked:
                return reranked
    except Exception:
        pass  # Fall back to original ordering

    return results[:top_k]


def semantic_search(
    conn: sqlite3.Connection,
    query: str,
    top_k: int = 10,
) -> list[dict[str, Any]]:
    """Full pipeline: FTS search -> Groq rerank -> return top results."""
    raw_results = text_search(conn, query, limit=50)
    if not raw_results:
        return []
    return rerank_with_groq(query, raw_results, top_k=top_k)


def get_popular_jellies(
    conn: sqlite3.Connection, limit: int = 20
) -> list[dict[str, Any]]:
    """Get the most popular jellies by views, with transcript availability."""
    rows = conn.execute(
        """
        SELECT
            j.id,
            j.title,
            j.all_views,
            j.likes_count,
            j.comments_count,
            j.tips_total,
            j.thumbnail_url,
            j.hls_master,
            j.duration,
            j.posted_at,
            CASE WHEN t.word_count > 0 THEN 1 ELSE 0 END as has_transcript
        FROM jellies j
        LEFT JOIN transcripts t ON t.jelly_id = j.id
        WHERE j.all_views > 0
        ORDER BY j.all_views DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]
