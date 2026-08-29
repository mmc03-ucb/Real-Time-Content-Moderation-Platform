"""
Every SQL query StreamGuard runs, in one file.

Keeping them together means you can read the whole database story in one sitting.
"""

import json
from typing import List, Optional

from moderation.rules.models import Rule


def _rows(conn, sql: str, args=()) -> List[dict]:
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def _one(conn, sql: str, args=()) -> Optional[dict]:
    rows = _rows(conn, sql, args)
    return rows[0] if rows else None


def _execute(conn, sql: str, args=()) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.rowcount


# ---------------------------------------------------------------- rules

def load_rules(conn) -> List[Rule]:
    """Read the live ruleset. Called on start up and again after any rule change."""
    rows = _rows(conn, """
        SELECT id, name, rule_type, pattern, threshold, action,
               priority, enabled, stream_id, version
        FROM rules
    """)
    return [Rule(
        id=r["id"], name=r["name"], rule_type=r["rule_type"], pattern=r["pattern"],
        threshold=r["threshold"], action=r["action"], priority=r["priority"],
        enabled=bool(r["enabled"]), stream_id=r["stream_id"], version=r["version"],
    ) for r in rows]


def set_rule_enabled(conn, rule_id: int, enabled: bool) -> int:
    """Turn a rule on or off and bump its version so the change is auditable."""
    return _execute(conn, """
        UPDATE rules SET enabled = %s, version = version + 1 WHERE id = %s
    """, (1 if enabled else 0, rule_id))


# ------------------------------------------------------------ decisions

def record_decision(conn, decision) -> bool:
    """
    Save a verdict. Returns False if we had already saved this message.

    Kafka can hand us the same message twice after a crash, so the unique
    msg_id plus INSERT IGNORE is what keeps the numbers honest.
    """
    changed = _execute(conn, """
        INSERT IGNORE INTO decisions
            (msg_id, stream_id, user_id, action, reason_code, rule_id,
             ml_score, strategy, latency_ms)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (decision.msg_id, decision.stream_id, decision.user_id, decision.action,
          decision.reason_code, decision.rule_id, decision.ml_score,
          decision.strategy, decision.latency_ms))
    return changed > 0


def decision_counts(conn, since_minutes: int = 60) -> List[dict]:
    return _rows(conn, """
        SELECT strategy, action, COUNT(*) AS total
        FROM decisions
        WHERE created_at > NOW() - INTERVAL %s MINUTE
        GROUP BY strategy, action
    """, (since_minutes,))


# --------------------------------------------------------- review queue

def enqueue_review(conn, decision, text: str, rule_hits: List[dict]) -> bool:
    """Put a borderline message in front of a human."""
    changed = _execute(conn, """
        INSERT IGNORE INTO review_items
            (msg_id, stream_id, user_id, text, ml_score, rule_hits_json, strategy)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (decision.msg_id, decision.stream_id, decision.user_id, text,
          decision.ml_score, json.dumps(rule_hits), decision.strategy))
    return changed > 0


def claim_next_item(conn, reviewer: str) -> Optional[dict]:
    """
    Hand the oldest waiting item to one moderator.

    The UPDATE is what stops two moderators grabbing the same message: only one
    of them can change a row that is still 'pending'.
    """
    candidate = _one(conn, """
        SELECT id FROM review_items
        WHERE status = 'pending' ORDER BY created_at LIMIT 1
    """)
    if not candidate:
        return None
    won = _execute(conn, """
        UPDATE review_items
        SET status = 'claimed', reviewer = %s, claimed_at = NOW()
        WHERE id = %s AND status = 'pending'
    """, (reviewer, candidate["id"]))
    if not won:
        return None
    return _one(conn, "SELECT * FROM review_items WHERE id = %s", (candidate["id"],))


def submit_review(conn, item_id: int, reviewer: str, decision: str) -> bool:
    """Record what the moderator chose. These become labels for tuning thresholds."""
    return _execute(conn, """
        UPDATE review_items
        SET status = 'done', decision = %s, reviewer = %s, decided_at = NOW()
        WHERE id = %s AND status <> 'done'
    """, (decision, reviewer, item_id)) > 0


def release_stale_claims(conn, older_than_minutes: int = 5) -> int:
    """Give back items a moderator claimed but never finished."""
    return _execute(conn, """
        UPDATE review_items SET status = 'pending', reviewer = NULL, claimed_at = NULL
        WHERE status = 'claimed' AND claimed_at < NOW() - INTERVAL %s MINUTE
    """, (older_than_minutes,))


def queue_depth(conn) -> int:
    row = _one(conn, "SELECT COUNT(*) AS n FROM review_items WHERE status = 'pending'")
    return int(row["n"]) if row else 0


def review_stats(conn) -> List[dict]:
    """
    Per strategy: how much review work it created and how often the model was
    overturned by a human. An overturn is our stand-in for a false positive.
    """
    return _rows(conn, """
        SELECT strategy,
               COUNT(*) AS reviewed,
               SUM(status = 'pending') AS waiting,
               SUM(decision = 'allow')  AS overturned,
               SUM(decision = 'delete') AS upheld
        FROM review_items
        GROUP BY strategy
    """)


# -------------------------------------------------------------- user risk

def bump_user_risk(conn, user_id: str, amount: float) -> None:
    """Nudge a user's reputation after a violation."""
    _execute(conn, """
        INSERT INTO users_risk (user_id, risk_score, violations, last_violation_at)
        VALUES (%s, %s, 1, NOW())
        ON DUPLICATE KEY UPDATE
            risk_score = risk_score + VALUES(risk_score),
            violations = violations + 1,
            last_violation_at = NOW()
    """, (user_id, amount))


# ------------------------------------------------------------- strategies

def load_strategies(conn) -> List[dict]:
    rows = _rows(conn, "SELECT name, config_json FROM strategies WHERE active = 1")
    for row in rows:
        if isinstance(row["config_json"], str):
            row["config_json"] = json.loads(row["config_json"])
    return rows
