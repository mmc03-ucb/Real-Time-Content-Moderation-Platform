"""
The moderator dashboard and the API behind it.

Two audiences: moderators working through the queue of borderline messages, and
whoever wants to see how the platform and its A/B policies are doing.

    uvicorn moderation.api.app:app --reload
"""

from pathlib import Path
from typing import Optional

from fastapi import Body, Depends, FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from moderation.defenses.client import build_redis
from moderation.obs import metrics
from moderation.rules.store import bump_version, read_version
from moderation.storage import dao, mysql

STATIC = Path(__file__).parent / "static"

app = FastAPI(title="StreamGuard", description="Live stream content moderation")

# Opened once and reused. Overridden with fakes in the tests.
_state = {}


def get_conn():
    if "conn" not in _state:
        _state["conn"] = mysql.connect()
    return _state["conn"]


def get_redis():
    if "redis" not in _state:
        _state["redis"] = build_redis()
    return _state["redis"]


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(STATIC / "index.html")


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics(conn=Depends(get_conn)):
    """Queue depth belongs to the API, since it is the one that owns the queue."""
    metrics.REVIEW_QUEUE_DEPTH.set(dao.queue_depth(conn))
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ------------------------------------------------------------ review queue

@app.post("/api/review/claim")
def claim(reviewer: str = "anonymous", conn=Depends(get_conn)):
    """
    Give this moderator the oldest waiting message.

    Returns 204 when the queue is empty, which the dashboard shows as
    "all caught up".
    """
    dao.release_stale_claims(conn)
    item = dao.claim_next_item(conn, reviewer)
    if item is None:
        return Response(status_code=204)
    return item


@app.post("/api/review/{item_id}")
def decide(item_id: int,
           decision: str = Body(..., embed=True),
           reviewer: str = Body("anonymous", embed=True),
           conn=Depends(get_conn)):
    """Record the moderator's call. These answers are the labels we tune against."""
    if decision not in ("allow", "delete"):
        raise HTTPException(400, "decision must be allow or delete")
    if not dao.submit_review(conn, item_id, reviewer, decision):
        raise HTTPException(404, "no such item, or it was already decided")
    return {"ok": True, "item_id": item_id, "decision": decision}


# ------------------------------------------------------------------ stats

@app.get("/api/stats")
def stats(conn=Depends(get_conn), redis_client=Depends(get_redis)):
    """Everything the dashboard's header and comparison table needs."""
    return {
        "queue_depth": dao.queue_depth(conn),
        "rules_version": read_version(redis_client),
        "decisions": dao.decision_counts(conn),
        "review": dao.review_stats(conn),
    }


# ------------------------------------------------------------------ rules

@app.get("/api/rules")
def list_rules(conn=Depends(get_conn)):
    return [r.__dict__ for r in dao.load_rules(conn)]


@app.post("/api/rules/{rule_id}/toggle")
def toggle_rule(rule_id: int,
                enabled: bool = Body(..., embed=True),
                conn=Depends(get_conn),
                redis_client=Depends(get_redis)):
    """
    Turn a rule on or off.

    Bumping the version key is the whole trick behind hot reload: workers see
    the new number within a few seconds and pick up the change themselves.
    """
    if not dao.set_rule_enabled(conn, rule_id, enabled):
        raise HTTPException(404, "no such rule")
    return {"ok": True, "rules_version": bump_version(redis_client)}
