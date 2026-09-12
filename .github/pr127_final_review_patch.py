from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise SystemExit(f"target not found: {label}")
    return text.replace(old, new, 1)


kr = Path("trader/kr/runner/trade_session_runner.py")
ktext = kr.read_text(encoding="utf-8")
ktext = replace_once(
    ktext,
    '''        if guard.status == "WARN_BALANCE_INCONSISTENT":
            status = guard.status
            summary_reason = guard.reason
            completed = guard.completed
            retryable = guard.retryable
''',
    '''        if guard.status == "WARN_BALANCE_INCONSISTENT":
            if close_phase_not_executed:
                logger.error(
                    "[KR_CLOSE][FAIL_PRESERVED] reason=CLOSE_PHASE_NOT_EXECUTED "
                    "post_balance_guard=%s guard_reason=%s",
                    guard.status, guard.reason,
                )
            else:
                status = guard.status
                summary_reason = guard.reason
                completed = guard.completed
                retryable = guard.retryable
''',
    "KR final balance guard precedence",
)
kr.write_text(ktext, encoding="utf-8")

us = Path("trader/us/runner/trade_tick_runner.py")
utext = us.read_text(encoding="utf-8")
needle = '''def _routed_order_notional(orders: list[dict]) -> float:
    total = 0.0
    for order in orders or []:
        if not _order_has_routed_submission_truth(order):
            continue
        intent = order.get("intent") if isinstance(order.get("intent"), dict) else {}
        try:
            total += float(intent.get("notional_usd", 0) or 0)
        except (TypeError, ValueError):
            continue
    return total
'''
replacement = needle + '''\n\ndef _routed_sell_notional(orders: list[dict]) -> float:
    sell_orders: list[dict] = []
    for order in orders or []:
        if not isinstance(order, dict):
            continue
        intent = order.get("intent") if isinstance(order.get("intent"), dict) else {}
        if str(intent.get("side") or "").upper() == "SELL":
            sell_orders.append(order)
    return _routed_order_notional(sell_orders)
'''
utext = replace_once(utext, needle, replacement, "US routed sell notional helper")
utext = replace_once(
    utext,
    '''    sell_notional_routed = _routed_order_notional(orders)
''',
    '''    sell_notional_routed = _routed_sell_notional(orders)
''',
    "US standard exit sell notional helper",
)
utext = replace_once(
    utext,
    '''    sell_notional_routed = float(exit_route_result.get("sell_notional_routed", 0.0) or 0.0)
''',
    '''    # Aggregate broker-routed SELL truth across both TQQQ Infinite and PB1 exits.
    # BUY orders from the Infinite sleeve are explicitly excluded.
    sell_notional_routed = _routed_sell_notional(orders)
''',
    "US combined infinite/PB1 sell notional",
)
us.write_text(utext, encoding="utf-8")

kr_test = Path("tests/kr/test_kr_pr127_close_contract.py")
kt = kr_test.read_text(encoding="utf-8")
if "def test_close_phase_failure_precedes_post_balance_guard" not in kt:
    kt += '''\n\ndef test_close_phase_failure_precedes_post_balance_guard():
    source = Path("trader/kr/runner/trade_session_runner.py").read_text(encoding="utf-8")
    guard_pos = source.index('if guard.status == "WARN_BALANCE_INCONSISTENT":')
    section = source[guard_pos:guard_pos + 650]
    assert 'if close_phase_not_executed:' in section
    assert 'post_balance_guard=%s' in section
    assert section.index('if close_phase_not_executed:') < section.index('status = guard.status')
'''
    kr_test.write_text(kt, encoding="utf-8")

us_test = Path("tests/us/test_us_pr127_residual_integrity.py")
ut = us_test.read_text(encoding="utf-8")
if "def test_routed_sell_notional_includes_infinite_sell_and_excludes_infinite_buy" not in ut:
    ut += '''\n\ndef test_routed_sell_notional_includes_infinite_sell_and_excludes_infinite_buy():
    from trader.us.runner.trade_tick_runner import _routed_sell_notional

    orders = [
        {"status": "ACK", "broker_submit": True, "intent": {"side": "SELL", "notional_usd": 700.0}},
        {"status": "ACK_DB_FAILED", "broker_submit": True, "intent": {"side": "SELL", "notional_usd": 300.0}},
        {"status": "ACK", "broker_submit": True, "intent": {"side": "BUY", "notional_usd": 900.0}},
        {"status": "BLOCKED", "broker_submit": False, "intent": {"side": "SELL", "notional_usd": 500.0}},
    ]
    assert _routed_sell_notional(orders) == 1000.0

    source = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    assert "sell_notional_routed = _routed_sell_notional(orders)" in source
'''
    us_test.write_text(ut, encoding="utf-8")
