from sqlalchemy.dialects import postgresql, sqlite

from trader.db.repos import LedgerEventsRepo


class _FakeEngine:
    def __init__(self, url: str, dialect) -> None:
        self.url = url
        self.dialect = dialect


def test_payload_as_of_expr_postgres() -> None:
    engine = _FakeEngine("postgresql+psycopg://", postgresql.dialect())
    repo = LedgerEventsRepo(engine)
    expr = repo._payload_as_of_expr()
    compiled = str(expr.compile(dialect=postgresql.dialect()))
    assert compiled


def test_payload_as_of_expr_sqlite() -> None:
    engine = _FakeEngine("sqlite://", sqlite.dialect())
    repo = LedgerEventsRepo(engine)
    expr = repo._payload_as_of_expr()
    compiled = str(expr.compile(dialect=sqlite.dialect()))
    assert compiled
