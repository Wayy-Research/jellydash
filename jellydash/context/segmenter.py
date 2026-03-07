"""Chunk transcripts into time-windowed segments for search."""

from __future__ import annotations

import json
import sqlite3
from typing import Any


def segment_transcript(
    words_json: str,
    window_seconds: float = 30.0,
    overlap_seconds: float = 10.0,
) -> list[dict[str, Any]]:
    """Split word-level transcript into overlapping time segments.

    Args:
        words_json: JSON string of word dicts with start/end times.
        window_seconds: Duration of each segment window.
        overlap_seconds: Overlap between consecutive windows.

    Returns:
        List of segment dicts with text, start_time, end_time.
    """
    try:
        words: list[dict[str, Any]] = json.loads(words_json) if words_json else []
    except (json.JSONDecodeError, TypeError):
        return []

    if not words:
        return []

    max_time = max(w.get("end", 0.0) for w in words)
    if max_time <= 0:
        # No timing info — return entire transcript as one segment
        text = " ".join(w.get("word", "") for w in words)
        return [{"text": text.strip(), "start_time": 0.0, "end_time": 0.0}] if text.strip() else []

    segments: list[dict[str, Any]] = []
    step = window_seconds - overlap_seconds
    if step <= 0:
        step = window_seconds

    t = 0.0
    while t < max_time:
        win_end = t + window_seconds
        chunk_words = [
            w.get("word", "")
            for w in words
            if w.get("start", 0.0) >= t and w.get("start", 0.0) < win_end
        ]
        text = " ".join(chunk_words).strip()
        if text:
            actual_end = min(win_end, max_time)
            segments.append({
                "text": text,
                "start_time": round(t, 2),
                "end_time": round(actual_end, 2),
            })
        t += step

    return segments


def build_segments_for_jelly(
    conn: sqlite3.Connection,
    jelly_id: str,
    window_seconds: float = 30.0,
    overlap_seconds: float = 10.0,
) -> int:
    """Build and store transcript segments for a single jelly.

    FTS5 index is kept in sync automatically via triggers.
    Returns the number of segments created.
    """
    row = conn.execute(
        "SELECT words_json FROM transcripts WHERE jelly_id = ?", (jelly_id,)
    ).fetchone()
    if not row or not row["words_json"]:
        return 0

    segments = segment_transcript(
        row["words_json"], window_seconds, overlap_seconds
    )
    if not segments:
        return 0

    # Use transaction to ensure atomicity of delete + insert
    with conn:
        conn.execute(
            "DELETE FROM transcript_segments WHERE jelly_id = ?", (jelly_id,)
        )
        for i, seg in enumerate(segments):
            conn.execute(
                """
                INSERT INTO transcript_segments
                    (jelly_id, segment_idx, text, start_time, end_time)
                VALUES (?, ?, ?, ?, ?)
                """,
                (jelly_id, i, seg["text"], seg["start_time"], seg["end_time"]),
            )

    return len(segments)


def build_all_segments(
    conn: sqlite3.Connection,
    window_seconds: float = 30.0,
    overlap_seconds: float = 10.0,
) -> int:
    """Build segments for all jellies that have transcripts but no segments.

    Returns total segments created.
    """
    rows = conn.execute(
        """
        SELECT t.jelly_id FROM transcripts t
        WHERE t.word_count > 0
        AND t.jelly_id NOT IN (
            SELECT DISTINCT jelly_id FROM transcript_segments
        )
        """
    ).fetchall()

    total = 0
    for row in rows:
        total += build_segments_for_jelly(
            conn, row["jelly_id"], window_seconds, overlap_seconds
        )
    return total
