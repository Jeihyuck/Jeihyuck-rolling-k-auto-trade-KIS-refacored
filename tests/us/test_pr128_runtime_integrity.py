from pathlib import Path

import pytest


def test_max_positions_zero_means_unlimited(monkeypatch):
    from trader.us.execution.risk_gate import check_position_count

    monkeypatch.setenv("US_MAX_POSITIONS", "0")
    check_position_count(10_000, symbol="AAPL")


def test_pending_buy_lookup_failure_fails_closed(monkeypatch):
    from trader.us.execution.risk_gate import RiskGateBlocked, check_pending_order
    from trader.us.db import repos

    monkeypatch.setenv("US_ORDER_ACCEPTED_IS_NOT_FILLED", "1")

    def boom(**_kwargs):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(repos, "has_pending_order_for_symbol_side", boom)
    with pytest.raises(RiskGateBlocked, match="pending_order_state_unknown"):
        check_pending_order("AAPL", "BUY", trade_date="2026-09-13")


def test_same_day_rebuy_lookup_failure_fails_closed(monkeypatch):
    from trader.us.execution.risk_gate import RiskGateBlocked, check_same_day_rebuy
    from trader.us.db import repos

    monkeypatch.setenv("US_BLOCK_REBUY_AFTER_SELL_SAME_DAY", "1")

    def boom():
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(repos, "load_today_symbols_sold", boom)
    with pytest.raises(RiskGateBlocked, match="same_day_rebuy_state_unknown"):
        check_same_day_rebuy("AAPL", "BUY")


def test_deploy_preflight_blocks_dirty_trading_code_by_default():
    text = Path("scripts/wsl/deploy-preflight.sh").read_text()
    assert "[DEPLOY][DIRTY_CODE][FAIL]" in text
    assert "action=BLOCK_TRADING" in text
    assert "ALLOW_DIRTY_TRADING_CODE" in text
    assert "reason=dirty_trading_code" in text


def test_practice_universe_provider_contract_is_live_first():
    from trader.universe.capabilities import providers_for_env

    assert providers_for_env("practice") == [
        "kis_marketcap_top", "seed_static", "emergency_seed"
    ]
