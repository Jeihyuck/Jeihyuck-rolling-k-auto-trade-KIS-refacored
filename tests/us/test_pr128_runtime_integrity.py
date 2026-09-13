from pathlib import Path
import pytest


def test_max_positions_zero_means_unlimited(monkeypatch):
    from trader.us.execution.risk_gate import check_position_count

    monkeypatch.setenv("US_MAX_POSITIONS", "0")
    check_position_count(10_000, symbol="AAPL")


def test_pending_buy_db_unavailable_fails_closed(monkeypatch):
    from trader.us.execution.risk_gate import RiskGateBlocked, check_pending_order
    from trader.us.db import strict_order_state

    monkeypatch.setenv("US_ORDER_ACCEPTED_IS_NOT_FILLED", "1")
    monkeypatch.setattr(
        strict_order_state,
        "has_pending_order_for_symbol_side_strict",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("us_persistent_db_unavailable")),
    )
    with pytest.raises(RiskGateBlocked, match="pending_order_state_unknown"):
        check_pending_order("AAPL", "BUY", trade_date="2026-09-13")


def test_pending_buy_query_exception_fails_closed(monkeypatch):
    from trader.us.execution.risk_gate import RiskGateBlocked, check_pending_order
    from trader.us.db import strict_order_state

    monkeypatch.setenv("US_ORDER_ACCEPTED_IS_NOT_FILLED", "1")
    monkeypatch.setattr(
        strict_order_state,
        "has_pending_order_for_symbol_side_strict",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("query failed")),
    )
    with pytest.raises(RiskGateBlocked, match="pending_order_state_unknown"):
        check_pending_order("AAPL", "BUY", trade_date="2026-09-13")


def test_same_day_rebuy_db_unavailable_fails_closed(monkeypatch):
    from trader.us.execution.risk_gate import RiskGateBlocked, check_same_day_rebuy
    from trader.us.db import strict_order_state

    monkeypatch.setenv("US_BLOCK_REBUY_AFTER_SELL_SAME_DAY", "1")
    monkeypatch.setattr(
        strict_order_state,
        "load_today_symbols_sold_strict",
        lambda: (_ for _ in ()).throw(RuntimeError("us_persistent_db_unavailable")),
    )
    with pytest.raises(RiskGateBlocked, match="same_day_rebuy_state_unknown"):
        check_same_day_rebuy("AAPL", "BUY")


def test_same_day_rebuy_query_exception_fails_closed(monkeypatch):
    from trader.us.execution.risk_gate import RiskGateBlocked, check_same_day_rebuy
    from trader.us.db import strict_order_state

    monkeypatch.setenv("US_BLOCK_REBUY_AFTER_SELL_SAME_DAY", "1")
    monkeypatch.setattr(
        strict_order_state,
        "load_today_symbols_sold_strict",
        lambda: (_ for _ in ()).throw(RuntimeError("query failed")),
    )
    with pytest.raises(RiskGateBlocked, match="same_day_rebuy_state_unknown"):
        check_same_day_rebuy("AAPL", "BUY")


def test_pending_buy_clear_state_allows(monkeypatch):
    from trader.us.execution.risk_gate import check_pending_order
    from trader.us.db import strict_order_state

    monkeypatch.setenv("US_ORDER_ACCEPTED_IS_NOT_FILLED", "1")
    monkeypatch.setattr(strict_order_state, "has_pending_order_for_symbol_side_strict", lambda **_kwargs: False)
    check_pending_order("AAPL", "BUY", trade_date="2026-09-13")


def test_pending_buy_existing_pending_blocks(monkeypatch):
    from trader.us.execution.risk_gate import RiskGateBlocked, check_pending_order
    from trader.us.db import strict_order_state

    monkeypatch.setenv("US_ORDER_ACCEPTED_IS_NOT_FILLED", "1")
    monkeypatch.setattr(strict_order_state, "has_pending_order_for_symbol_side_strict", lambda **_kwargs: True)
    with pytest.raises(RiskGateBlocked, match="pending_order_exists"):
        check_pending_order("AAPL", "BUY", trade_date="2026-09-13")


def test_same_day_rebuy_clear_state_allows(monkeypatch):
    from trader.us.execution.risk_gate import check_same_day_rebuy
    from trader.us.db import strict_order_state

    monkeypatch.setenv("US_BLOCK_REBUY_AFTER_SELL_SAME_DAY", "1")
    monkeypatch.setattr(strict_order_state, "load_today_symbols_sold_strict", lambda: set())
    check_same_day_rebuy("AAPL", "BUY")


def test_same_day_rebuy_existing_sell_blocks(monkeypatch):
    from trader.us.execution.risk_gate import RiskGateBlocked, check_same_day_rebuy
    from trader.us.db import strict_order_state

    monkeypatch.setenv("US_BLOCK_REBUY_AFTER_SELL_SAME_DAY", "1")
    monkeypatch.setattr(strict_order_state, "load_today_symbols_sold_strict", lambda: {"AAPL"})
    with pytest.raises(RiskGateBlocked, match="same_day_rebuy_block"):
        check_same_day_rebuy("AAPL", "BUY")


def test_sell_pending_lookup_exception_preserves_exit_liveness(monkeypatch):
    from trader.us.execution import risk_gate
    from trader.us.db import repos

    monkeypatch.setenv("US_AGENT_ENABLED", "true")
    monkeypatch.setenv("TRADING_REGION", "US")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
    monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("DISABLE_REAL_TRADING", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "0")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("US_ORDER_ARMED", "1")
    monkeypatch.setenv("ALLOW_REAL_ORDER", "1")
    monkeypatch.setattr(
        repos,
        "has_pending_order_for_symbol_side",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("db degraded")),
    )
    risk_gate.assert_order_allowed(
        {
            "symbol": "AAPL",
            "exchange": "NASDAQ",
            "side": "SELL",
            "qty": 1,
            "notional_usd": 100.0,
            "client_order_key": "sell-safe-1",
            "available_qty": 5,
        },
        available_cash_usd=1000.0,
        total_portfolio_usd=1000.0,
        allowed_symbols={"AAPL"},
        current_position_symbols={"AAPL"},
    )


def test_deploy_preflight_blocks_dirty_trading_code_by_default():
    text = Path("scripts/wsl/deploy-preflight.sh").read_text()
    assert "[DEPLOY][DIRTY_CODE][FAIL]" in text
    assert "action=BLOCK_TRADING" in text
    assert "ALLOW_DIRTY_TRADING_CODE" in text
    assert "reason=dirty_trading_code" in text


def test_deploy_preflight_checks_dirty_before_first_sync():
    text = Path("scripts/wsl/deploy-preflight.sh").read_text()
    assert text.index("dirty_code=") < text.index('_deploy_sync_market_code "${market^^}" "$trade_date"')


def test_emergency_override_preserves_dirty_before_sync():
    text = Path("scripts/wsl/deploy-preflight.sh").read_text()
    assert "export SYNC_MARKET_PRESERVE_DIRTY=1" in text
    sync_text = Path("scripts/wsl/sync-market-code.sh").read_text()
    assert 'if [[ "${SYNC_MARKET_PRESERVE_DIRTY:-0}" == "1" ]]' in sync_text
    assert "action=PRESERVE_DIRTY_WORKTREE" in sync_text
    override_block = sync_text.split('if [[ "${SYNC_MARKET_PRESERVE_DIRTY:-0}" == "1" ]]')[1].split("else", 1)[0]
    assert "git checkout -f -B dual-agent origin/dual-agent" not in override_block
    assert "git reset --hard origin/dual-agent" not in override_block


def test_sync_still_resets_when_clean():
    sync_text = Path("scripts/wsl/sync-market-code.sh").read_text()
    assert "git checkout -f -B dual-agent origin/dual-agent" in sync_text
    assert "git reset --hard origin/dual-agent" in sync_text


def test_practice_universe_provider_contract_is_live_first():
    from trader.universe.capabilities import providers_for_env

    assert providers_for_env("practice") == [
        "kis_marketcap_top", "seed_static", "emergency_seed"
    ]
