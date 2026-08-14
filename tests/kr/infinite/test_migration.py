import re
from pathlib import Path
from trader.db.migrate import split_postgres_sql
SQL=Path("migrations/0049_add_kr_infinite_state.sql").read_text().upper()
def test_additive_idempotent_migration():
 assert "CREATE TABLE IF NOT EXISTS KR_INFINITE_STATE" in SQL and "CREATE TABLE IF NOT EXISTS KR_INFINITE_ORDER_INTENTS" in SQL
 assert SQL.count("CREATE INDEX IF NOT EXISTS")==3 and "UNIQUE" in SQL and not re.search(r"\b(ALTER|DROP|TRUNCATE|DELETE)\b",SQL)
 assert len(split_postgres_sql(SQL))==5
def test_existing_tables_not_mutated_and_version_contract():
 for table in ("ORDERS","FILLS","POSITIONS","US_ORDERS","US_FILLS","US_POSITIONS","US_TQQQ_INFINITE_STATE"):assert not re.search(rf"(?:ALTER|DROP|TRUNCATE|DELETE\s+FROM|UPDATE)\s+(?:TABLE\s+)?{table}\b",SQL)
 assert re.fullmatch(r"\d{4}_.+\.sql",Path("migrations/0049_add_kr_infinite_state.sql").name)
