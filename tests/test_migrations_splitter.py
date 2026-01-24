import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.db.migrate import split_postgres_sql


def test_splitter_handles_dollar_quote_block():
    sql = """
    DO $$
    BEGIN
      PERFORM 1;
    END
    $$;
    SELECT 1;
    """
    statements = split_postgres_sql(sql)
    assert len(statements) == 2
    assert "DO $$" in statements[0]
    assert "PERFORM 1;" in statements[0]
    assert statements[1].lstrip().startswith("SELECT 1")


def test_splitter_handles_tagged_dollar_quote():
    sql = """
    CREATE OR REPLACE FUNCTION test() RETURNS void AS $func$
    BEGIN
      RAISE NOTICE 'hi;';
    END;
    $func$;
    SELECT 2;
    """
    statements = split_postgres_sql(sql)
    assert len(statements) == 2
    assert "$func$" in statements[0]
    assert "RAISE NOTICE 'hi;';" in statements[0]
    assert statements[1].lstrip().startswith("SELECT 2")


def test_splitter_ignores_semicolons_in_comments():
    sql = "SELECT 1; -- comment;\nSELECT 2; /* block; comment */ SELECT 3;"
    statements = split_postgres_sql(sql)
    assert len(statements) == 3
    assert statements[0].lstrip().startswith("SELECT 1")
    assert "SELECT 2" in statements[1]
    assert "SELECT 3" in statements[2]
