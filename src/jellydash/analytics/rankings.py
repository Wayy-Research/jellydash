"""User ranking aggregation queries."""

from __future__ import annotations

import sqlite3


def refresh_user_stats(conn: sqlite3.Connection) -> int:
    """Recompute user_stats from jellies + jelly_participants.

    Returns:
        Number of users with stats.
    """
    cur = conn.cursor()
    cur.execute("DELETE FROM user_stats")
    cur.execute(
        """
        INSERT INTO user_stats (
            participant_id, total_jellies, total_views, total_likes,
            total_comments, total_tips, avg_views, avg_likes,
            avg_comments, avg_duration
        )
        SELECT
            jp.participant_id,
            COUNT(*) as total_jellies,
            COALESCE(SUM(j.all_views), 0),
            COALESCE(SUM(j.likes_count), 0),
            COALESCE(SUM(j.comments_count), 0),
            COALESCE(SUM(j.tips_total), 0.0),
            COALESCE(AVG(j.all_views), 0.0),
            COALESCE(AVG(j.likes_count), 0.0),
            COALESCE(AVG(j.comments_count), 0.0),
            COALESCE(AVG(j.duration), 0.0)
        FROM jelly_participants jp
        JOIN jellies j ON j.id = jp.jelly_id
        GROUP BY jp.participant_id
        """
    )
    conn.commit()
    row = conn.execute("SELECT COUNT(*) as c FROM user_stats").fetchone()
    return row["c"] if row else 0


def get_ranked_users(
    conn: sqlite3.Connection,
    metric: str = "total_views",
    topic: str | None = None,
    limit: int = 50,
) -> list[dict[str, object]]:
    """Get users ranked by a metric, optionally filtered by topic.

    Args:
        conn: Database connection.
        metric: Column name to rank by.
        topic: Optional topic to filter by (via jelly_topics).
        limit: Max results.

    Returns:
        List of dicts with rank, user info, and metric value.
    """
    allowed_metrics = {
        "total_views",
        "total_likes",
        "total_comments",
        "total_tips",
        "total_jellies",
        "avg_views",
        "avg_likes",
    }
    if metric not in allowed_metrics:
        raise ValueError(f"Invalid metric: {metric}")

    if topic:
        rows = conn.execute(
            f"""
            SELECT
                p.username, p.full_name, p.pfp_url, p.id as participant_id,
                COUNT(DISTINCT j.id) as total_jellies,
                COALESCE(SUM(j.all_views), 0) as total_views,
                COALESCE(SUM(j.likes_count), 0) as total_likes,
                COALESCE(SUM(j.comments_count), 0) as total_comments,
                COALESCE(SUM(j.tips_total), 0.0) as total_tips,
                COALESCE(AVG(j.all_views), 0.0) as avg_views,
                COALESCE(AVG(j.likes_count), 0.0) as avg_likes,
                RANK() OVER (ORDER BY {metric} DESC) as rank
            FROM jelly_participants jp
            JOIN jellies j ON j.id = jp.jelly_id
            JOIN jelly_topics jt ON jt.jelly_id = j.id
            JOIN participants p ON p.id = jp.participant_id
            WHERE jt.topic = ?
            GROUP BY jp.participant_id
            ORDER BY {metric} DESC
            LIMIT ?
            """,  # noqa: S608
            (topic, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT
                p.username, p.full_name, p.pfp_url, p.id as participant_id,
                us.*,
                RANK() OVER (ORDER BY us.{metric} DESC) as rank
            FROM user_stats us
            JOIN participants p ON p.id = us.participant_id
            ORDER BY us.{metric} DESC
            LIMIT ?
            """,  # noqa: S608
            (limit,),
        ).fetchall()

    return [dict(r) for r in rows]
