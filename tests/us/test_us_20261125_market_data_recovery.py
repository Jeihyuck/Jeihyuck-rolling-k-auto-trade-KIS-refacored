"""11/25 incident regression: quote venue, empty HTTP, and recovery reason."""
import pytest
from trader.us.symbols import resolve_exchange, resolve_quote_exchange, resolve_quote_exchange_code, resolve_order_exchange
from trader.us.prep_effective import benchmark_recovery_required
from trader.us.data_provider import USDataProvider
from trader.us.db.price_daily_repo import reset_us_daily_memory

@pytest.mark.parametrize("symbol", "SPY DIA IWM RSP XLE XLF XLI XLK XLP XLU XLV".split())
def test_etf_quote_route_separate_from_order(symbol):
    assert resolve_exchange(symbol) == "NYSE"
    assert resolve_quote_exchange(symbol) == "AMEX"
    assert resolve_quote_exchange_code(symbol) == "AMS"
    assert resolve_order_exchange(symbol) == "NYSE"

@pytest.mark.parametrize("symbol", "QQQ SMH QQQM SOXX TQQQ KHC".split())
def test_nasdaq_quote_route_unchanged(symbol):
    assert resolve_quote_exchange_code(symbol) == "NAS"
    assert resolve_order_exchange(symbol) == "NASDAQ"

def test_empty_http_is_attempted_but_failed():
    reset_us_daily_memory()
    class EmptyClient:
        def get_us_daily_price_history(self, *args, **kwargs):
            return []
    p = USDataProvider(offline=True)
    p._client = EmptyClient()
    p._offline = False
    result = p.get_completed_daily_prices_result("SPY", "AMEX", trade_date="2026-11-24", required_bars=260, allow_http_sync=True)
    assert result["quality"] == "STALE"
    assert result["http_sync_attempted"] is True
    assert result["http_sync_succeeded"] is False
    assert result["http_sync_call_count"] == 1

def test_benchmark_recovery_does_not_override_risk_off():
    assert benchmark_recovery_required({"recoverable_contract": True, "benchmark_daily_failed": True})
    assert not benchmark_recovery_required({"recoverable_contract": True, "trade_block_reason": "RISK_OFF"})
    assert not benchmark_recovery_required({"benchmark_daily_failed": True})


def test_entry_revision_requires_artifact_db_and_manifest(monkeypatch, tmp_path):
    from trader.us.prep_effective import entry_revision_proof
    from trader.us.run_manifest import pin_run_revision
    date = "2026-11-24"
    revision = "a" * 40
    monkeypatch.setenv("US_RUN_MANIFEST_DIR", str(tmp_path))
    data = {key: 1 for key in ("entry_can_proceed", "exit_can_proceed", "trade_can_proceed",
                                "final30_trade_ready", "contract_ok", "allow_new_buy")}
    data.update(contract_version="us_sector_rotation_v3",
                market_regime_version="us_leading_regime_v1", run_revision=revision)
    prep = {"status": "OK", "result": data}
    artifact = {"entry_can_proceed": 1, "run_revision": revision}
    assert not entry_revision_proof(date, artifact, prep)[0]
    pin_run_revision(date, revision)
    assert entry_revision_proof(date, artifact, prep)[0]
    assert not entry_revision_proof(date, {**artifact, "run_revision": "b" * 40}, prep)[0]
    assert not entry_revision_proof(date, {**artifact, "entry_can_proceed": 0}, prep)[0]
    assert entry_revision_proof(date, {"entry_can_proceed": 0}, {"result": {"entry_can_proceed": 0}})[0]


def test_tp_refresh_replaces_first_price_alias(monkeypatch):
    import time
    from trader.us.execution.tp_signal_quote_refresh import refresh_tp_source_quotes
    from trader.us.market_state_overlay import _price
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_SHARED_KIS_LIMITER_ENABLED", "1")
    class Provider:
        def get_isolated_exit_quote(self, *args, **kwargs):
            return {"last": 100, "_quote_asof_epoch": time.time(),
                    "_quote_source": "KIS_LIVE", "_quote_quality": "FRESH"}
    old = {"symbol": "ABBV", "strategy_owner": "US_STANDARD", "qty": 10,
           "last_price": 110, "resolved_quote_asof": time.time()-60}
    fresh = refresh_tp_source_quotes([old], Provider())[0]
    assert _price(fresh) == 100
    assert _price(old) == 110
