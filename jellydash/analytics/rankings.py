"""User ranking aggregation queries."""

from __future__ import annotations

import sqlite3


def refresh_user_stats(conn: sqlite3.Connection) -> int:
    """Recompute user_stats from jellies + jelly_participants.

    Computes base aggregates, views_per_post, jelly_score (0-100),
    and flags rising stars.

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
            avg_comments, avg_duration, views_per_post
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
            COALESCE(AVG(j.duration), 0.0),
            CASE WHEN COUNT(*) > 0
                 THEN CAST(COALESCE(SUM(j.all_views), 0) AS REAL) / COUNT(*)
                 ELSE 0.0 END
        FROM jelly_participants jp
        JOIN jellies j ON j.id = jp.jelly_id
        GROUP BY jp.participant_id
        """
    )

    # Compute jelly_score using PERCENT_RANK (0-100 scale)
    # 60% views_per_post percentile + 25% avg_likes percentile + 15% avg_comments
    cur.execute(
        """
        UPDATE user_stats SET jelly_score = (
            SELECT 100.0 * (
                0.60 * ranks.vpp_pct + 0.25 * ranks.al_pct + 0.15 * ranks.ac_pct
            )
            FROM (
                SELECT participant_id,
                    PERCENT_RANK() OVER (ORDER BY views_per_post) as vpp_pct,
                    PERCENT_RANK() OVER (ORDER BY avg_likes) as al_pct,
                    PERCENT_RANK() OVER (ORDER BY avg_comments) as ac_pct
                FROM user_stats
            ) ranks
            WHERE ranks.participant_id = user_stats.participant_id
        )
        """
    )

    # Flag rising stars: first seen in last 30 days, >=3 posts,
    # views_per_post above platform average
    cur.execute(
        """
        UPDATE user_stats SET is_rising_star = 1
        WHERE participant_id IN (
            SELECT us.participant_id
            FROM user_stats us
            JOIN participants p ON p.id = us.participant_id
            WHERE p.first_seen_at >= strftime('%Y-%m-%dT%H:%M:%SZ',
                  datetime('now', '-30 days'))
              AND us.total_jellies >= 3
              AND us.views_per_post > (
                  SELECT AVG(views_per_post) FROM user_stats
                  WHERE total_jellies >= 1
              )
        )
        """
    )

    conn.commit()
    row = conn.execute("SELECT COUNT(*) as c FROM user_stats").fetchone()
    return row["c"] if row else 0


def get_rising_stars(
    conn: sqlite3.Connection, limit: int = 20
) -> list[dict[str, object]]:
    """Get users flagged as rising stars, ranked by jelly_score."""
    rows = conn.execute(
        """
        SELECT
            p.username, p.full_name, p.pfp_url, p.id as participant_id,
            us.total_jellies, us.total_views, us.views_per_post,
            us.jelly_score, us.avg_likes, us.avg_comments,
            p.first_seen_at
        FROM user_stats us
        JOIN participants p ON p.id = us.participant_id
        WHERE us.is_rising_star = 1
        ORDER BY us.jelly_score DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


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
        "views_per_post",
        "jelly_score",
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
                CASE WHEN COUNT(DISTINCT j.id) > 0
                     THEN CAST(COALESCE(SUM(j.all_views), 0) AS REAL)
                          / COUNT(DISTINCT j.id)
                     ELSE 0.0 END as views_per_post,
                0.0 as jelly_score,
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
