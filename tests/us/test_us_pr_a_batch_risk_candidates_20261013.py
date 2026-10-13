"""PR-A: preserve owner/epoch/lifecycle identity while batching exit risk history.

These tests do not contact KIS or mutate Supabase.
"""
from __future__ import annotations

from trader.us.db import repos
from trader.us.pb1 import us_exit_position_resolver as resolver


DATE = "2026-10-12"


def _identity(symbol: str) -> dict:
    return {
        "symbol": symbol,
        "env": "practice",
        "account_id": "practice:demo",
        "trading_epoch_id": "epoch-current",
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": f"lifecycle-{symbol}",
    }


def _history(symbol: str, *, epoch: str = "epoch-current", high: float = 123.0) -> dict:
    identity = _identity(symbol)
    identity["trading_epoch_id"] = epoch
    lifecycle = {
        "lifecycle_id": identity["position_lifecycle_id"],
        "env": identity["env"],
        "account_id": identity["account_id"],
        "trading_epoch_id": epoch,
        "strategy_owner": identity["strategy_owner"],
        "opened_at": "2026-09-22T13:30:00+00:00",
        "opened_trade_date": "2026-09-22",
        "holding_trade_days": 12,
        "high_watermark": high,
        "is_open": True,
    }
    return {
        "symbol": symbol,
        "trade_date": DATE,
        "trading_epoch_id": epoch,
        "state": {"lifecycle": lifecycle},
        "updated_at": "2026-10-12T13:30:00+00:00",
    }


class _FakeEngine:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def begin(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        self.calls.append((str(query), params))
        return self

    def mappings(self):
        return self

    def all(self):
        return self.rows


def test_batch_uses_one_bounded_sql_query_for_nineteen_symbols(monkeypatch):
    symbols = [f"SYM{i:02}" for i in range(19)]
    engine = _FakeEngine([_history(s) for s in symbols])
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    monkeypatch.setattr(repos, "_ensure_us_position_risk_state_table", lambda conn: None)
    result = repos.load_us_position_risk_state_candidates_batch(
        symbols + [symbols[0].lower()], DATE
    )
    assert set(result) == set(symbols)
    assert all(len(result[s]) == 1 for s in symbols)
    assert len(engine.calls) == 1
    sql, params = engine.calls[0]
    assert "PARTITION BY symbol" in sql
    assert "ORDER BY trade_date DESC, updated_at DESC" in sql
    assert "candidate_rank <= 2000" in sql
    assert params == {"symbols": symbols, "trade_date": DATE}


def test_batch_query_failure_falls_back_to_exact_legacy_per_symbol_lookup(monkeypatch):
    class _FailEngine(_FakeEngine):
        def execute(self, query, params):
            raise RuntimeError("database unavailable during batch")

    engine = _FailEngine([])
    fallback = []
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    monkeypatch.setattr(repos, "_ensure_us_position_risk_state_table", lambda conn: None)

    def old_lookup(symbol, trade_date):
        fallback.append((symbol, trade_date))
        return [_history(symbol)]

    monkeypatch.setattr(repos, "load_us_position_risk_state_candidates", old_lookup)
    result = repos.load_us_position_risk_state_candidates_batch(["MSFT", "AAPL"], DATE)
    assert fallback == [("AAPL", DATE), ("MSFT", DATE)]
    assert len(result["AAPL"]) == len(result["MSFT"]) == 1


def test_resolver_preserves_exact_epoch_owner_lifecycle_and_high_watermark(monkeypatch):
    calls = []
    rows_by_symbol = {
        "AAPL": [_history("AAPL", epoch="wrong-epoch", high=999.0),
                 _history("AAPL", high=120.0)],
        "MSFT": [_history("MSFT", high=230.0)],
    }

    def batch(symbols, trade_date):
        calls.append((tuple(symbols), trade_date))
        return {symbol: rows_by_symbol.get(symbol, []) for symbol in symbols}

    monkeypatch.setattr(repos, "load_us_position_risk_state_candidates_batch", batch)
    monkeypatch.setattr(
        repos, "load_us_position_risk_state_candidates",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("N+1 risk lookup")),
    )
    monkeypatch.setattr(resolver, "_identity_is_current", lambda identity: True)

    positions, meta = resolver.enrich_us_positions_for_exit(
        [
            {**_identity("AAPL"), "qty": 2, "avg_price_usd": 100.0,
             "current_price_usd": 110.0},
            {**_identity("MSFT"), "qty": 1, "avg_price_usd": 200.0,
             "current_price_usd": 205.0},
        ],
        trade_date=DATE,
    )
    assert calls == [(("AAPL", "MSFT"), DATE)]
    assert meta["ok"] == 2
    assert [p["high_watermark"] for p in positions] == [120.0, 230.0]
    assert all(p["opened_at"] == "2026-09-22T13:30:00+00:00" for p in positions)
    assert all(p["position_lifecycle_view"]["identity_status"] == "IDENTIFIED" for p in positions)


def test_batch_does_not_adopt_prior_lifecycle_for_reentry(monkeypatch):
    wrong = _history("AAPL", high=900.0)
    wrong["state"]["lifecycle"]["lifecycle_id"] = "old-lifecycle"
    monkeypatch.setattr(
        repos, "load_us_position_risk_state_candidates_batch",
        lambda symbols, trade_date: {"AAPL": [wrong], "MSFT": []},
    )
    monkeypatch.setattr(resolver, "_identity_is_current", lambda identity: True)
    rows, _ = resolver.enrich_us_positions_for_exit(
        [
            {**_identity("AAPL"), "qty": 1, "avg_price_usd": 100.0,
             "current_price_usd": 101.0},
            {**_identity("MSFT"), "qty": 1, "avg_price_usd": 200.0,
             "current_price_usd": 202.0},
        ], trade_date=DATE,
    )
    assert rows[0]["high_watermark"] != 900.0
    assert rows[0]["entry_price"] == 100.0
    assert rows[1]["entry_price"] == 200.0


def test_single_position_keeps_original_history_lookup(monkeypatch):
    count = []
    monkeypatch.setattr(resolver, "_identity_is_current", lambda identity: True)
    monkeypatch.setattr(
        repos, "load_us_position_risk_state_candidates",
        lambda symbol, date: count.append((symbol, date)) or [_history(symbol)],
    )
    monkeypatch.setattr(
        repos, "load_us_position_risk_state_candidates_batch",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("single-symbol batch")),
    )
    rows, _ = resolver.enrich_us_positions_for_exit(
        [{**_identity("AAPL"), "qty": 1, "avg_price_usd": 100.0}],
        trade_date=DATE,
    )
    assert count == [("AAPL", DATE)]
    assert rows[0]["high_watermark"] == 123.0
