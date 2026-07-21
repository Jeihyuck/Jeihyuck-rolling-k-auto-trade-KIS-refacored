"""Regression coverage for the 2026-07-21 psycopg3 JSONB bind failure."""
from datetime import date

from sqlalchemy.dialects import postgresql

from trader.us.db import repos


def test_mark_filled_by_reconcile_actual_fill_uses_typed_jsonb_binds():
    """The KIS cumulative AMD SELL snapshot must compile without unknown JSONB args."""
    stmt = repos._mark_filled_by_reconcile_stmt()
    sql = str(stmt.compile(dialect=postgresql.dialect()))

    assert "CAST(%(ts)s AS text)" in sql
    assert "CAST(%(qty)s AS integer)" in sql
    assert "CAST(%(price)s AS numeric)" in sql
    assert "CAST(%(td)s AS date)" in sql
    assert isinstance(stmt._bindparams["qty"].type, repos.sa.Integer)
    assert isinstance(stmt._bindparams["price"].type, repos.sa.Numeric)
    assert isinstance(stmt._bindparams["td"].type, repos.sa.Date)

    # Exact production failure inputs: this is the parameter shape psycopg3 sees.
    params = {
        "qty": 1, "price": 529.245, "cok": "e741f4c726b86941c78ed3f0",
        "remaining": 0, "requested": 1,
        "ts": "2026-07-21T13:41:32.353765+00:00",
        "td": date.fromisoformat("2026-07-21"), "order_no": "0000037900",
        "symbol": "AMD", "side": "SELL",
    }
    compiled = stmt.compile(dialect=postgresql.dialect())
    assert compiled.construct_params(params)["ts"] == params["ts"]
    assert compiled.construct_params(params)["td"] == params["td"]
