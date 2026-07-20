"""Regression coverage for authoritative US-position snapshot persistence."""

from __future__ import annotations

from contextlib import contextmanager


def _position(symbol: str, qty: int = 1) -> dict:
    return {"symbol": symbol, "exchange": "NASDAQ", "qty": qty}


def test_stale_close_sql_casts_source_and_symbols(monkeypatch):
    """PostgreSQL must know the types of jsonb and ANY parameters."""
    from trader.us.db import repos

    statements: list[tuple[str, dict]] = []

    class Connection:
        def execute(self, statement, params):
            statements.append((str(statement), params))

    class Engine:
        @contextmanager
        def begin(self):
            yield Connection()

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: Engine())
    repos.save_position_snapshot(
        [_position("AAPL")],
        trade_date="2026-07-20",
        balance_fetch_status="OK",
        balance_parse_status="OK",
        authoritative_positions=True,
        preserve_previous_positions=False,
        close_source="kis_reconcile_balance",
    )

    stale_close_sql, params = statements[-1]
    assert "CAST(:source AS text)" in stale_close_sql
    assert "ANY(CAST(:symbols AS text[]))" in stale_close_sql
    assert params["source"] == "kis_reconcile_balance"
    assert params["symbols"] == ["AAPL"]


def test_authoritative_snapshot_keeps_kis_symbols_and_closes_only_stale(monkeypatch):
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "_MEM_POSITIONS", [
        {"as_of": "2026-07-20", "symbol": "AAPL", "qty": 4, "meta": {}},
        {"as_of": "2026-07-20", "symbol": "AMD", "qty": 2, "meta": {}},
    ])

    repos.save_position_snapshot(
        [_position("AAPL", 4)],
        trade_date="2026-07-20",
        balance_fetch_status="OK",
        balance_parse_status="OK",
        authoritative_positions=True,
        preserve_previous_positions=False,
        close_source="kis_reconcile_balance",
    )

    aapl = next(row for row in repos._MEM_POSITIONS if row["symbol"] == "AAPL")
    amd = next(row for row in repos._MEM_POSITIONS if row["symbol"] == "AMD")
    assert aapl["qty"] == 4
    assert amd["qty"] == 0
    assert amd["meta"]["position_status"] == "CLOSED_BY_AUTHORITATIVE_BALANCE"
    assert amd["meta"]["close_source"] == "kis_reconcile_balance"


def test_empty_authoritative_balance_allows_full_close(monkeypatch):
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "_MEM_POSITIONS", [{"as_of": "2026-07-20", "symbol": "AMD", "qty": 2, "meta": {}}])

    repos.save_position_snapshot(
        [], trade_date="2026-07-20", balance_fetch_status="OK", balance_parse_status="OK",
        authoritative_positions=True, preserve_previous_positions=False,
    )

    assert repos._MEM_POSITIONS[0]["qty"] == 0


def test_non_authoritative_empty_balance_preserves_existing_positions(monkeypatch):
    """A failed/parse-failed KIS balance must not be treated as an empty balance."""
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "_MEM_POSITIONS", [{"as_of": "2026-07-20", "symbol": "AMD", "qty": 2, "meta": {}}])

    repos.save_position_snapshot(
        [], trade_date="2026-07-20", balance_fetch_status="FAILED", balance_parse_status="ERROR",
        authoritative_positions=False, preserve_previous_positions=True,
    )

    assert repos._MEM_POSITIONS[0]["qty"] == 2
