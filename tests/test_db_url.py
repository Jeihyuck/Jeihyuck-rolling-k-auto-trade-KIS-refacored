import pytest

from trader.db.engine import get_db_url


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    # 테스트 전 DB URL 관련 env 전부 제거
    for k in [
        "PBCORE_DB_URL",
        "DATABASE_URL",
        "TRADER_DB_URL",
        "DB_URL",
        "SQLALCHEMY_DATABASE_URL",
    ]:
        monkeypatch.delenv(k, raising=False)


def test_accept_postgresql_psycopg_scheme(monkeypatch):
    monkeypatch.setenv(
        "PBCORE_DB_URL", "postgresql+psycopg://user:pass@localhost:5432/postgres"
    )
    assert get_db_url().startswith("postgresql+psycopg://")


def test_reject_sqlite(monkeypatch):
    monkeypatch.setenv("PBCORE_DB_URL", "sqlite:///bot_state/db/pbcore.sqlite3")
    with pytest.raises(RuntimeError) as e:
        get_db_url()
    assert "SQLite is forbidden" in str(e.value)


def test_reject_non_postgres_scheme(monkeypatch):
    monkeypatch.setenv("PBCORE_DB_URL", "mysql://user:pass@localhost:3306/db")
    with pytest.raises(RuntimeError) as e:
        get_db_url()
    assert "Postgres only" in str(e.value)


def test_missing_db_url(monkeypatch):
    with pytest.raises(RuntimeError) as e:
        get_db_url()
    assert "DB URL missing" in str(e.value)
