from trader.db.locks import acquire_advisory_xact_lock, release_advisory_xact_lock


class _Scalar:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _Connection:
    def __init__(self):
        self.sql = []
        self.rolled_back = False

    def execute(self, statement, params=None):
        text = str(statement)
        self.sql.append(text)
        return _Scalar("pg_try_advisory_xact_lock" in text)

    def in_transaction(self):
        return True

    def rollback(self):
        self.rolled_back = True


def test_transaction_lock_uses_xact_function_and_releases_on_transaction_end(monkeypatch):
    monkeypatch.setenv("LOCK_ACQUIRE_RETRIES", "1")
    conn = _Connection()

    assert acquire_advisory_xact_lock(conn, key=912345678, context="market=KR session=am") is True
    release_advisory_xact_lock(conn, key=912345678, context="market=KR session=am")

    assert any("pg_try_advisory_xact_lock" in sql for sql in conn.sql)
    assert conn.rolled_back is True


def test_stale_holder_requires_idle_advisory_transaction(monkeypatch):
    from trader.db.locks import _stale_holder

    monkeypatch.setenv("KR_LOCK_STALE_XACT_SEC", "300")
    stale, reason = _stale_holder({"state": "idle in transaction", "query": "SELECT pg_try_advisory_lock($1)", "xact_age_seconds": 301})
    assert stale is True
    assert reason == "idle_in_transaction_advisory_lock"
