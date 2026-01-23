from sqlalchemy import text

from trader.db.engine import make_engine


def assert_db_ready() -> None:
    engine = make_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
