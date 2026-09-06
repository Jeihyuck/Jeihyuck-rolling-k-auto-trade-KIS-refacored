from __future__ import annotations

from trader.kr.pb1.order_gate import resolve_order_precheck_gate_reasons


def test_resolve_order_precheck_gate_reasons_buy_blocks_fail_soft_entry():
    assert resolve_order_precheck_gate_reasons(
        trading_day=True,
        order_allowed=True,
        side="BUY",
        market_window_name="open",
        force_entry_window_override=False,
        session_recovery_continue=False,
        am_recovery_continue=False,
        force_block_live=False,
        intended_live=True,
        strategy_mode="LIVE",
        balance_fail_soft_active=True,
        balance_fail_soft_entry_allowed=False,
        balance_fail_soft_exit_allowed=False,
    ) == ["balance_fail_soft_entry_disabled"]


def test_resolve_order_precheck_gate_reasons_sell_allows_fail_soft_exit_without_order_block():
    assert resolve_order_precheck_gate_reasons(
        trading_day=True,
        order_allowed=False,
        side="SELL",
        market_window_name="open",
        force_entry_window_override=False,
        session_recovery_continue=False,
        am_recovery_continue=False,
        force_block_live=False,
        intended_live=True,
        strategy_mode="LIVE",
        balance_fail_soft_active=True,
        balance_fail_soft_entry_allowed=False,
        balance_fail_soft_exit_allowed=True,
    ) == []


def test_resolve_order_precheck_gate_reasons_dedupes_and_applies_window_and_live_blocks():
    assert resolve_order_precheck_gate_reasons(
        trading_day=False,
        order_allowed=False,
        side="SELL",
        market_window_name="after",
        force_entry_window_override=False,
        session_recovery_continue=False,
        am_recovery_continue=False,
        force_block_live=True,
        intended_live=False,
        strategy_mode="DIAG",
        balance_fail_soft_active=False,
        balance_fail_soft_entry_allowed=False,
        balance_fail_soft_exit_allowed=False,
    ) == [
        "nontrading_day",
        "order_blocked",
        "window_blocked",
        "live_gate_blocked",
    ]
