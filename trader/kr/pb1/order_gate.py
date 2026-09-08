from __future__ import annotations


def resolve_order_precheck_gate_reasons(
    *,
    trading_day: bool,
    order_allowed: bool,
    side: str,
    market_window_name: str,
    force_entry_window_override: bool,
    session_recovery_continue: bool,
    am_recovery_continue: bool,
    force_block_live: bool,
    intended_live: bool,
    strategy_mode: str,
    balance_fail_soft_active: bool,
    balance_fail_soft_entry_allowed: bool,
    balance_fail_soft_exit_allowed: bool,
) -> list[str]:
    reasons: list[str] = []
    if not trading_day:
        reasons.append("nontrading_day")
    side_upper = side.upper()
    if side_upper == "BUY" and balance_fail_soft_active and not balance_fail_soft_entry_allowed:
        reasons.append("balance_fail_soft_entry_disabled")
    elif side_upper == "SELL" and balance_fail_soft_active and balance_fail_soft_exit_allowed:
        pass
    elif not order_allowed:
        reasons.append("order_blocked")
    if market_window_name == "after" and not (force_entry_window_override or session_recovery_continue or am_recovery_continue):
        reasons.append("window_blocked")
    if force_block_live or not intended_live or strategy_mode == "DIAG":
        reasons.append("live_gate_blocked")
    deduped: list[str] = []
    for reason in reasons:
        if reason not in deduped:
            deduped.append(reason)
    return deduped
