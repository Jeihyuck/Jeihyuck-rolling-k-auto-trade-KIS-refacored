from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


def _simulate_postgres_watchlist_row():
    from trader.us.db.repos import _merge_us_daily_metrics_meta

    prep_row = {
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "score": 0.92,
        "score_final": 0.92,
        "entry_style_selected": "pb1_pullback",
        "pullback_score": 0.81,
        "breakout_score": 0.12,
        "momentum_score": 0.43,
        "reasons": ["ENTRY_PULLBACK"],
        "filters_passed": ["score", "risk"],
        "score_breakdown": {"pullback": 0.81},
        "explanation_quality": "FULL",
        "rank_final30": 1,
        "theme_cluster": "TECH",
        "market_regime": "GROWTH_LEADERSHIP",
        "meta": {},
    }
    meta = _merge_us_daily_metrics_meta(prep_row)
    # This is the actual PostgreSQL load shape: provenance lives in meta JSON,
    # not as dedicated us_watchlist columns.
    return {
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "strategy": "pb1_pullback",
        "score": 0.92,
        "meta": meta,
        "prep_status": "OK",
        "run_id": "p0-contract-chain",
        "data_source": "kis",
    }


def test_us_final30_db_reload_preserves_provenance_and_creates_live_buy(monkeypatch):
    from trader.us.db import repos
    from trader.us.score_columns import (
        canonicalize_us_watchlist_row,
        validate_us_entry_provenance_contract,
    )
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    from trader.us.execution.order_router import route_order

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0.01")
    monkeypatch.setenv("US_MAX_NEW_ENTRIES_PER_TICK", "1")
    monkeypatch.setenv("US_MAX_ORDER_USD", "5000")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "1")
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "50000")
    monkeypatch.setenv("KIS_ENV", "practice")

    db_row = _simulate_postgres_watchlist_row()
    canonical = canonicalize_us_watchlist_row(db_row)

    assert canonical["entry_style_selected"] == "pb1_pullback"
    assert canonical["pullback_score"] == 0.81
    assert canonical["reasons"] == ["ENTRY_PULLBACK"]
    assert validate_us_entry_provenance_contract([db_row])["ok"] is True

    class Provider:
        def get_current_price(self, symbol, exchange):
            return {"last": 100.0}

    diagnostics = {}
    intents = generate_entry_intents(
        tickers=None,
        provider=Provider(),
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=0,
        capital_usd_cap=10000.0,
        now=datetime(2026, 9, 18, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        max_new_entries=1,
        watchlist_entries=[canonical],
        current_position_symbols=set(),
        diagnostics=diagnostics,
    )

    assert len(intents) == 1
    assert intents[0]["symbol"] == "AAPL"
    assert intents[0]["entry_style_selected"] == "ENTRY_PULLBACK"
    assert not [
        item for item in diagnostics.get("blocked", [])
        if item.get("reason") == "ENTRY_EXPLAIN_CONTRACT_ERROR"
    ]

    routed = route_order(
        intents[0],
        signal_only=True,
        current_position_symbols=set(),
        allowed_symbols={"AAPL"},
    )
    assert routed["status"] == "SIGNAL_ONLY"
    contract = routed["intent"]["meta"]["entry_exit_contract"]
    assert contract["entry_provenance"]["entry_style_selected"] == "ENTRY_PULLBACK"
    assert routed["intent"]["meta"]["entry_exit_contract_sha256"] == contract["sha256"]


def test_us_live_provenance_validator_rejects_db_row_without_style():
    from trader.us.score_columns import validate_us_entry_provenance_contract

    broken = _simulate_postgres_watchlist_row()
    broken["meta"].pop("entry_style_selected", None)
    result = validate_us_entry_provenance_contract([broken])
    assert result["ok"] is False
    assert result["invalid_count"] == 1
    assert result["invalid"][0]["symbol"] == "AAPL"


def test_us_persisted_position_exposure_never_collapses_to_zero():
    from trader.us.portfolio_cluster_guard import resolve_position_market_value_usd

    persisted = {"symbol": "MSFT", "qty": 10, "current_px": 500.0, "avg_cost": 490.0}
    assert resolve_position_market_value_usd(persisted) == 5000.0

    cost_only = {"symbol": "MSFT", "qty": 10, "avg_cost": 490.0}
    assert resolve_position_market_value_usd(cost_only) == 4900.0


def test_us_health_and_preflight_have_contract_integrity_gates():
    tick = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    preflight = Path("scripts/wsl/check-us-prep-before-am.sh").read_text(encoding="utf-8")

    assert "[US_ENTRY][CONTRACT_INTEGRITY_FAIL]" in tick
    assert 'entry_degraded_reason = "entry_contract_integrity_fail"' in tick
    assert "validate_us_entry_provenance_contract" in preflight
    assert "DB_LIVE_CONTRACT_FAIL" in preflight


def test_us_intentional_balance_interval_skip_is_not_a_failure_marker():
    tick = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    assert 'intentional_balance_skip = str(recon.get("status") or "").upper() == "SKIPPED_BALANCE_RECONCILE"' in tick
    assert "balance_fetch_failed = bool(\n        not intentional_balance_skip" in tick
