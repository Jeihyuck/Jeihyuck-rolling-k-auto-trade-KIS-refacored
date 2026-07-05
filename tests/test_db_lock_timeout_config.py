from trader.db.engine import _connect_args_for_db_url


def test_general_postgres_connection_timeout_options(monkeypatch):
    monkeypatch.setenv("DB_LOCK_TIMEOUT_MS", "5000")
    monkeypatch.setenv("DB_STATEMENT_TIMEOUT_MS", "15000")
    monkeypatch.setenv("DB_IDLE_IN_TX_SESSION_TIMEOUT_MS", "15000")
    args = _connect_args_for_db_url("postgresql+psycopg://u:p@localhost/db")
    opts = args["options"]
    assert "lock_timeout=5000" in opts
    assert "statement_timeout=15000" in opts
    assert "idle_in_transaction_session_timeout=15000" in opts


def test_lock_connection_timeout_options(monkeypatch):
    monkeypatch.setenv("DB_LOCK_CONN_LOCK_TIMEOUT_MS", "5000")
    monkeypatch.setenv("DB_LOCK_CONN_STATEMENT_TIMEOUT_MS", "0")
    monkeypatch.setenv("DB_LOCK_CONN_IDLE_IN_TX_SESSION_TIMEOUT_MS", "0")
    args = _connect_args_for_db_url("postgresql+psycopg://u:p@localhost/db", lock_connection=True)
    opts = args["options"]
    assert "lock_timeout=5000" in opts
    assert "statement_timeout=0" in opts
    assert "idle_in_transaction_session_timeout=0" in opts
