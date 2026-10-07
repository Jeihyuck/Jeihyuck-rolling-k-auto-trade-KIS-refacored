"""Completed market context, risk batching and timing: PB1 2026-10-06."""
from datetime import date, timedelta

from trader.us import market_state_overlay as market
from trader.us.runner.trade_tick_runner import calculate_latency_accounting
from trader.us.db import repos


def _daily_bars(end=date(2026, 10, 5), n=260):
    return [{
        "date": (end - timedelta(days=n - i - 1)).isoformat(),
        "close": 100.0 + i,
    } for i in range(n)]


class CompletedProvider:
    def __init__(self, missing=None):
        self.calls = []
        self.missing = missing

    def get_completed_daily_prices(self, symbol, exchange, trade_date, required_bars,
                                   allow_http_sync=False):
        self.calls.append(symbol)
        if symbol == self.missing:
            return []
        return _daily_bars()


def test_completed_market_context_no_repeated_daily_io_without_version_change():
    provider = CompletedProvider()
    warnings = []
    first = market._market_returns(
        provider, "2026-10-06", warnings, data_version="version-1",
    )
    context = first["_completed_market_context"]
    assert context["quality"] == "OK"
    assert context["expected_completed_date"] == "2026-10-05"
    assert len(provider.calls) == len(market._MARKET_RETURN_SYMBOLS)
    second = market._market_returns(
        provider, "2026-10-06", [], completed_market_context=context, data_version="version-1",
    )
    assert second["_completed_market_cache_hit"] is True
    assert len(provider.calls) == len(market._MARKET_RETURN_SYMBOLS)
    assert second["qqq_20d_return"] == first["qqq_20d_return"]
    changed = market._market_returns(
        provider, "2026-10-06", [], completed_market_context=context, data_version="version-2",
    )
    assert changed["_completed_market_cache_hit"] is False
    assert len(provider.calls) == 2 * len(market._MARKET_RETURN_SYMBOLS)


def test_failed_bootstrap_must_retry_and_then_recover():
    provider = CompletedProvider(missing="QQQ")
    first = market._market_returns(provider, "2026-10-06", [], data_version="v")
    assert first["_completed_market_context"]["quality"] == "INCOMPLETE"
    provider.missing = None
    recovered = market._market_returns(
        provider, "2026-10-06", [], data_version="v",
        completed_market_context=first["_completed_market_context"],
    )
    assert recovered["_completed_market_cache_hit"] is False
    assert recovered["_completed_market_context"]["quality"] == "OK"


def test_completed_context_wrong_trade_date_never_reused():
    provider = CompletedProvider()
    previous = market._market_returns(provider, "2026-10-06", [], data_version="v")["_completed_market_context"]
    rows = market._market_returns(
        provider, "2026-10-07", [], data_version="v",
        completed_market_context=previous,
    )
    assert rows["_completed_market_cache_hit"] is False


def test_batch_latest_risk_load_executes_one_db_select(monkeypatch):
    sql_calls = []
    class Rows:
        def mappings(self): return self
        def all(self): return [
            {"symbol": "AAPL", "trade_date": date(2026, 10, 6),
             "state": {"trend": {"ma20": 200, "lifecycle_id": "lc-aapl"}}},
            {"symbol": "MSFT", "trade_date": date(2026, 10, 6),
             "state": {"trend": {"ma20": 500, "lifecycle_id": "lc-msft"}}},
        ]
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def execute(self, sql, params):
            sql_calls.append((str(sql), params))
            return Rows()
    class Engine:
        def begin(self): return Connection()
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: Engine())
    monkeypatch.setattr(repos, "_us_state_epoch_id", lambda *_args: "epoch-test")
    monkeypatch.setattr(repos, "_ensure_us_position_risk_state_table", lambda *_args: None)
    result = repos.load_latest_us_position_risk_states(
        ["AAPL", "MSFT", "AAPL"], "2026-10-06",
    )
    assert len(sql_calls) == 1
    assert set(result) == {"AAPL", "MSFT"}
    assert sql_calls[0][1]["symbols"] == ["AAPL", "MSFT"]


def test_accounting_excludes_nested_http_metrics():
    stages = {
        "position_reconcile_ms": 20000,
        "fill_fetch_stage_ms": 10000,
        "fill_fetch_ms": 10000,
        "fill_persist_ms": 30000,
        "order_reconcile_ms": 15000,
        "exit_engine_ms": 35000,
        "market_state_ms": 20000,
        "tqqq_infinite_ms": 2000,
        "entry_engine_ms": 10000,
        "order_route_ms": 5000,
        "cash_fetch_ms": 3000,
        "balance_snapshot_ms": 20000, # nested
        "quote_http_calls": 1000,     # not a duration
    }
    accounted, residual = calculate_latency_accounting(200000, stages)
    assert accounted == 150000
    assert residual == 50000
