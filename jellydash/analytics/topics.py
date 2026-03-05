"""TF-IDF topic extraction from transcripts."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS, TfidfVectorizer

STOP_WORDS_EXTRA: list[str] = [
    # Spoken-language fillers
    "um", "uh", "uh huh", "hmm", "huh", "ah", "eh", "er", "mm",
    "oh", "wow", "whoa", "ooh", "yep", "yup", "nah", "nope",
    # Common interjections / discourse markers
    "like", "know", "yeah", "right", "okay", "ok", "sure", "well",
    "anyway", "anyways", "basically", "literally", "actually", "really",
    "honestly", "seriously", "definitely", "obviously", "apparently",
    "exactly", "absolutely", "probably", "maybe", "perhaps",
    # Contractions and informal forms
    "gonna", "gotta", "wanna", "kinda", "sorta", "coulda", "shoulda",
    "woulda", "ain", "don", "didn", "doesn", "isn", "wasn", "weren",
    "won", "wouldn", "couldn", "hasn", "haven", "hadn", "shouldn",
    "ve", "ll", "re", "let",
    # Common verbs that add noise
    "just", "got", "get", "getting", "going", "go", "come", "came",
    "said", "say", "saying", "tell", "told", "think", "thought",
    "mean", "want", "need", "look", "looking", "see", "saw",
    "make", "making", "made", "take", "taking", "took",
    "put", "try", "trying", "tried", "give", "gave", "keep",
    "feel", "felt", "start", "started", "happen", "happened",
    # Generic nouns
    "thing", "things", "stuff", "lot", "lots", "way", "time",
    "people", "person", "guy", "guys", "man", "woman",
    "day", "year", "point", "part", "kind",
]

STOP_WORDS_ALL: list[str] = sorted(set(ENGLISH_STOP_WORDS) | set(STOP_WORDS_EXTRA))


def extract_topics(
    texts: list[str],
    top_n: int = 30,
    max_features: int = 5000,
) -> list[tuple[str, float]]:
    """Extract top topics from a list of transcript texts using TF-IDF.

    Args:
        texts: List of transcript full texts.
        top_n: Number of top terms to return.
        max_features: Max vocabulary size.

    Returns:
        List of (term, score) sorted by score descending.
    """
    if not texts or len(texts) < 2:
        return []

    vectorizer = TfidfVectorizer(
        ngram_range=(1, 2),
        max_features=max_features,
        stop_words=STOP_WORDS_ALL,
        min_df=2,
        max_df=0.85,
    )

    try:
        tfidf_matrix = vectorizer.fit_transform(texts)
    except ValueError:
        return []

    feature_names = vectorizer.get_feature_names_out()
    mean_scores: np.ndarray[Any, np.dtype[np.floating[Any]]] = np.asarray(
        tfidf_matrix.mean(axis=0)
    ).flatten()

    top_indices = mean_scores.argsort()[::-1][:top_n]
    return [(str(feature_names[i]), float(mean_scores[i])) for i in top_indices]


def extract_jelly_topics(
    text: str,
    vectorizer: TfidfVectorizer,
    top_n: int = 5,
) -> list[tuple[str, float]]:
    """Extract top terms for a single jelly using a pre-fit vectorizer.

    Args:
        text: Single transcript text.
        vectorizer: Pre-fit TfidfVectorizer.
        top_n: Number of top terms.

    Returns:
        List of (term, relevance) pairs.
    """
    if not text.strip():
        return []

    try:
        vec = vectorizer.transform([text])
    except Exception:
        return []

    feature_names = vectorizer.get_feature_names_out()
    scores: np.ndarray[Any, np.dtype[np.floating[Any]]] = np.asarray(
        vec.toarray()
    ).flatten()
    top_indices = scores.argsort()[::-1][:top_n]
    return [
        (str(feature_names[i]), float(scores[i])) for i in top_indices if scores[i] > 0
    ]


def _period_start(period: str) -> str:
    """Get ISO start date for a named period."""
    now = datetime.now(UTC)
    if period == "24h":
        dt = now - timedelta(hours=24)
    elif period == "7d":
        dt = now - timedelta(days=7)
    elif period == "30d":
        dt = now - timedelta(days=30)
    else:  # all_time
        return "2020-01-01T00:00:00Z"
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def refresh_topics(
    conn: sqlite3.Connection,
    period: str = "all_time",
    top_n: int = 30,
) -> int:
    """Recompute topics for a time period.

    Returns:
        Number of topics stored.
    """
    ps = _period_start(period)

    if period == "all_time":
        rows = conn.execute(
            "SELECT jelly_id, full_text FROM transcripts WHERE full_text != ''"
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT t.jelly_id, t.full_text
            FROM transcripts t
            JOIN jellies j ON j.id = t.jelly_id
            WHERE t.full_text != '' AND j.posted_at >= ?
            """,
            (ps,),
        ).fetchall()

    texts = [r["full_text"] for r in rows]
    jelly_ids = [r["jelly_id"] for r in rows]

    topics = extract_topics(texts, top_n=top_n)

    # Store global topics
    conn.execute(
        "DELETE FROM topics WHERE period = ? AND period_start = ?", (period, ps)
    )
    for term, score in topics:
        conn.execute(
            """
            INSERT INTO topics (topic, score, period, period_start)
            VALUES (?, ?, ?, ?)
            """,
            (term, score, period, ps),
        )

    # Per-jelly topic assignment
    if texts and len(texts) >= 2:
        vectorizer = TfidfVectorizer(
            ngram_range=(1, 2),
            max_features=5000,
            stop_words=STOP_WORDS_ALL,
            min_df=2,
            max_df=0.85,
        )
        try:
            vectorizer.fit(texts)
        except ValueError:
            conn.commit()
            return len(topics)

        for jid, text in zip(jelly_ids, texts):
            jt = extract_jelly_topics(text, vectorizer, top_n=5)
            for term, relevance in jt:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO jelly_topics (jelly_id, topic, relevance)
                    VALUES (?, ?, ?)
                    """,
                    (jid, term, relevance),
                )

    conn.commit()
    return len(topics)
