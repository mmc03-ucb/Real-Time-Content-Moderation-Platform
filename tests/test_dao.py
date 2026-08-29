"""
Tests for the query building that is easy to get wrong. Anything that needs a
real MySQL is checked by running the platform, not here.
"""

from moderation.storage import dao


class RecordingCursor:
    """Captures the SQL and arguments instead of running them."""

    def __init__(self, log):
        self.log = log
        self.rowcount = 1

    def execute(self, sql, args=()):
        self.log.append((" ".join(sql.split()), list(args)))

    def executemany(self, sql, args):
        self.log.append((" ".join(sql.split()), list(args)))

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class RecordingConn:
    def __init__(self):
        self.log = []

    def cursor(self):
        return RecordingCursor(self.log)


def test_repeat_offenders_in_one_batch_are_added_up_once():
    conn = RecordingConn()
    dao.bump_user_risks(conn, [("u1", 1.0), ("u1", 1.0), ("u2", 1.0)])
    sql, args = conn.log[0]
    assert sql.count("(%s, %s, %s, NOW())") == 2   # two users, not three rows
    assert args == ["u1", 2.0, 2, "u2", 1.0, 1]    # u1 counted twice


def test_risk_rows_are_written_in_a_fixed_order():
    """Workers must take locks in the same order, or MySQL deadlocks them."""
    conn = RecordingConn()
    dao.bump_user_risks(conn, [("zoe", 1.0), ("adam", 1.0), ("mia", 1.0)])
    _, args = conn.log[0]
    assert args[0::3] == ["adam", "mia", "zoe"]


def test_nothing_is_written_when_there_is_nothing_to_write():
    conn = RecordingConn()
    assert dao.bump_user_risks(conn, []) == 0
    assert dao.record_decisions(conn, []) == 0
    assert dao.enqueue_reviews(conn, []) == 0
    assert dao.existing_msg_ids(conn, []) == set()
    assert conn.log == []
