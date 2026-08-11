from pathlib import Path

from trader.db.migrate import split_postgres_sql


SQL_PATH = Path("migrations/0048_add_us_tqqq_infinite_state.sql")


def test_migration_is_additive_idempotent_and_single_table():
    sql = SQL_PATH.read_text()
    upper = sql.upper()
    assert "CREATE TABLE IF NOT EXISTS US_TQQQ_INFINITE_STATE" in upper
    assert "CREATE INDEX IF NOT EXISTS" in upper
    assert "ALTER TABLE" not in upper
    for destructive in ("DROP ", "TRUNCATE ", "DELETE ", "UPDATE "):
        assert destructive not in upper
    assert "TQQQ_FILLS" not in upper


def test_migration_parser_and_schema_migration_version_contract():
    statements = split_postgres_sql(SQL_PATH.read_text())
    assert len(statements) == 2
    assert SQL_PATH.name == "0048_add_us_tqqq_infinite_state.sql"
    migrate_source = Path("trader/db/migrate.py").read_text()
    assert "INSERT INTO schema_migrations(version)" in migrate_source


def test_stable_code_compatibility_no_existing_table_mutation():
    sql = SQL_PATH.read_text().lower()
    existing = ("us_orders", "us_fills", "us_positions", "orders", "fills", "positions")
    assert all(f"alter table {table}" not in sql for table in existing)
