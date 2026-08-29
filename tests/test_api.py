import pytest
from fastapi.testclient import TestClient

from moderation.api import app as api_module
from moderation.rules.models import Rule


class FakeReviewDao:
    """In-memory version of the queue and rules tables."""

    def __init__(self):
        self.items = [
            {"id": 1, "msg_id": "m1", "stream_id": "s1", "user_id": "u1",
             "text": "you are an idiot", "ml_score": 0.7, "rule_hits_json": "[]",
             "strategy": "A", "status": "pending", "reviewer": None, "decision": None},
        ]
        self.rules = [Rule(1, "slurs", "keyword", "idiot", None, "delete", 10,
                           True, None, 1)]
        self.released = 0

    def release_stale_claims(self, conn, older_than_minutes=5):
        self.released += 1
        return 0

    def claim_next_item(self, conn, reviewer):
        for item in self.items:
            if item["status"] == "pending":
                item.update(status="claimed", reviewer=reviewer)
                return item
        return None

    def submit_review(self, conn, item_id, reviewer, decision):
        for item in self.items:
            if item["id"] == item_id and item["status"] != "done":
                item.update(status="done", decision=decision, reviewer=reviewer)
                return True
        return False

    def queue_depth(self, conn):
        return sum(1 for i in self.items if i["status"] == "pending")

    def decision_counts(self, conn, since_minutes=60):
        return [{"strategy": "A", "action": "delete", "total": 5}]

    def review_stats(self, conn):
        return [{"strategy": "A", "reviewed": 1, "waiting": 0,
                 "overturned": 1, "upheld": 0}]

    def load_rules(self, conn):
        return self.rules

    def set_rule_enabled(self, conn, rule_id, enabled):
        found = [r for r in self.rules if r.id == rule_id]
        if not found:
            return 0
        self.rules = [r if r.id != rule_id else
                      Rule(r.id, r.name, r.rule_type, r.pattern, r.threshold, r.action,
                           r.priority, enabled, r.stream_id, r.version + 1)
                      for r in self.rules]
        return 1


@pytest.fixture
def client(fake_redis, monkeypatch):
    dao = FakeReviewDao()
    monkeypatch.setattr(api_module, "dao", dao)
    api_module.app.dependency_overrides[api_module.get_conn] = lambda: None
    api_module.app.dependency_overrides[api_module.get_redis] = lambda: fake_redis
    yield TestClient(api_module.app), dao, fake_redis
    api_module.app.dependency_overrides.clear()


def test_health_check(client):
    http, _, _ = client
    assert http.get("/healthz").json() == {"status": "ok"}


def test_claiming_hands_over_the_waiting_message(client):
    http, _, _ = client
    item = http.post("/api/review/claim?reviewer=alice").json()
    assert item["text"] == "you are an idiot"
    assert item["reviewer"] == "alice"


def test_an_empty_queue_answers_with_no_content(client):
    http, _, _ = client
    http.post("/api/review/claim")
    assert http.post("/api/review/claim").status_code == 204


def test_two_moderators_never_get_the_same_message(client):
    http, _, _ = client
    first = http.post("/api/review/claim?reviewer=alice").json()
    second = http.post("/api/review/claim?reviewer=bob")
    assert first["id"] == 1
    assert second.status_code == 204


def test_submitting_a_decision_closes_the_item(client):
    http, dao, _ = client
    http.post("/api/review/claim?reviewer=alice")
    res = http.post("/api/review/1", json={"decision": "allow", "reviewer": "alice"})
    assert res.json()["ok"] is True
    assert dao.items[0]["decision"] == "allow"


def test_only_allow_or_delete_are_accepted(client):
    http, _, _ = client
    assert http.post("/api/review/1", json={"decision": "maybe"}).status_code == 400


def test_deciding_twice_is_rejected(client):
    http, _, _ = client
    http.post("/api/review/1", json={"decision": "allow"})
    assert http.post("/api/review/1", json={"decision": "delete"}).status_code == 404


def test_stats_report_the_queue_and_the_strategies(client):
    http, _, _ = client
    stats = http.get("/api/stats").json()
    assert stats["queue_depth"] == 1
    assert stats["decisions"][0]["total"] == 5
    assert stats["review"][0]["overturned"] == 1


def test_turning_a_rule_off_tells_the_workers(client):
    http, dao, redis_client = client
    res = http.post("/api/rules/1/toggle", json={"enabled": False})
    assert res.json()["rules_version"] == 1
    assert dao.rules[0].enabled is False
    # Version bumped again on the next change, which is what workers watch.
    assert http.post("/api/rules/1/toggle", json={"enabled": True}).json()["rules_version"] == 2


def test_toggling_a_rule_that_does_not_exist_is_a_404(client):
    http, _, _ = client
    assert http.post("/api/rules/99/toggle", json={"enabled": False}).status_code == 404


def test_the_dashboard_page_is_served(client):
    http, _, _ = client
    body = http.get("/").text
    assert "StreamGuard" in body
