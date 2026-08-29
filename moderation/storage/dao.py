"""
Every SQL query StreamGuard runs, in one file.

Keeping them together means you can read the whole database story in one sitting.
"""

import json
import time
from typing import List, Optional

import pymysql

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


def _execute_retrying(conn, sql: str, args=(), attempts: int = 3) -> int:
    """
    Run a statement that several workers might run at the same time.

    MySQL breaks a deadlock by rejecting one side and asking it to try again,
    which is exactly what this does.
    """
    for attempt in range(attempts):
        try:
            return _execute(conn, sql, args)
        except pymysql.err.OperationalError as exc:
            deadlock = exc.args[0] == 1213
            if not deadlock or attempt == attempts - 1:
                raise
            time.sleep(0.05 * (attempt + 1))
    return 0


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

def existing_msg_ids(conn, msg_ids: List[str]) -> set:
    """
    Which of these messages have we already decided?

    Kafka can hand us the same message twice after a crash, so this is what
    keeps the counts honest. One query for the whole batch.
    """
    if not msg_ids:
        return set()
    placeholders = ", ".join(["%s"] * len(msg_ids))
    rows = _rows(conn, f"SELECT msg_id FROM decisions WHERE msg_id IN ({placeholders})",
                 tuple(msg_ids))
    return {r["msg_id"] for r in rows}


def record_decisions(conn, decisions) -> int:
    """
    Save a batch of verdicts in one statement.

    INSERT IGNORE on top of the unique msg_id is the safety net: if two workers
    somehow handle the same message, only one row survives.
    """
    if not decisions:
        return 0
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT IGNORE INTO decisions
                (msg_id, stream_id, user_id, action, reason_code, rule_id,
                 ml_score, strategy, latency_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [(d.msg_id, d.stream_id, d.user_id, d.action, d.reason_code, d.rule_id,
               d.ml_score, d.strategy, d.latency_ms) for d in decisions])
        return cur.rowcount


def decision_counts(conn, since_minutes: int = 60) -> List[dict]:
    return _rows(conn, """
        SELECT strategy, action, COUNT(*) AS total
        FROM decisions
        WHERE created_at > NOW() - INTERVAL %s MINUTE
        GROUP BY strategy, action
    """, (since_minutes,))


# --------------------------------------------------------- review queue

def enqueue_reviews(conn, items: List[tuple]) -> int:
    """Put borderline messages in front of a human. Each item is (decision, text)."""
    if not items:
        return 0
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT IGNORE INTO review_items
                (msg_id, stream_id, user_id, text, ml_score, rule_hits_json, strategy)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, [(d.msg_id, d.stream_id, d.user_id, text, d.ml_score,
               json.dumps(d.rule_hits), d.strategy) for d, text in items])
        return cur.rowcount


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

def bump_user_risks(conn, users: List[tuple]) -> int:
    """
    Durable copy of user reputation. Each entry is (user_id, amount), and a user
    who appears several times in a batch is added up once.

    The VALUES list is built by hand rather than with executemany, because the
    driver sends an upsert one row at a time. One statement for the batch turned
    out to be about thirty times faster.

    Rows are sorted by user id so that workers writing the same users take their
    locks in the same order. Without that, two workers can deadlock.
    """
    if not users:
        return 0

    totals = {}
    for user_id, amount in users:
        score, count = totals.get(user_id, (0.0, 0))
        totals[user_id] = (score + amount, count + 1)

    rows = ", ".join(["(%s, %s, %s, NOW())"] * len(totals))
    args = [value for user_id in sorted(totals)
            for value in (user_id, totals[user_id][0], totals[user_id][1])]
    return _execute_retrying(conn, f"""
        INSERT INTO users_risk (user_id, risk_score, violations, last_violation_at)
        VALUES {rows}
        ON DUPLICATE KEY UPDATE
            risk_score = risk_score + VALUES(risk_score),
            violations = violations + VALUES(violations),
            last_violation_at = NOW()
    """, args)


# ------------------------------------------------------------- strategies

def load_strategies(conn) -> List[dict]:
    rows = _rows(conn, "SELECT name, config_json FROM strategies WHERE active = 1")
    for row in rows:
        if isinstance(row["config_json"], str):
            row["config_json"] = json.loads(row["config_json"])
    return rows
