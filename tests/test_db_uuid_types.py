import sqlalchemy as sa

from trader.db.schema import schema_for_url


def test_universe_id_uses_string_for_postgres():
    url = "postgresql+psycopg://user:pass@localhost:5432/db"
    schema = schema_for_url(url)
    col_type = schema.universe_runs.c.run_id.type
    assert isinstance(col_type, sa.String)


def test_universe_id_uses_string_for_sqlite():
    url = "sqlite:///./dev.db"
    schema = schema_for_url(url)
    col_type = schema.universe_runs.c.run_id.type
    assert isinstance(col_type, sa.String)
