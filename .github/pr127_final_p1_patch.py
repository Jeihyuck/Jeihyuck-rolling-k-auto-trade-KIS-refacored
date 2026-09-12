from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise SystemExit(f"target not found: {label}")
    return text.replace(old, new, 1)


us = Path("trader/us/runner/trade_tick_runner.py")
text = us.read_text(encoding="utf-8")
text = replace_once(
    text,
    '''    ack_cnt = sum(1 for o in orders if o["status"] == "ACK")
    dry_cnt = sum(1 for o in orders if o["status"] == "DRY_RUN")
    exit_closed_cnt = sum(1 for o in orders if o["status"] == "OK_EXIT_POSITION_CLOSED")
    sell_reconcile_pending_cnt = sum(1 for o in orders if o["status"] == "WARN_SELL_REJECT_RECONCILE_PENDING")
    blocked_cnt = sum(1 for o in orders if o["status"] in {"BLOCKED", "WARN_DUPLICATE_EXIT_BLOCKED"})
    signal_only_cnt = sum(1 for o in orders if o["status"] == "SIGNAL_ONLY")
    reject_cnt = sum(1 for o in orders if o["status"] == "REJECT")
    err_cnt = sum(1 for o in orders if o["status"] == "ERROR")
    ack_db_failed_cnt = sum(1 for o in orders if o.get("status") == "ACK_DB_FAILED")

    if not offline and (ack_cnt > 0 or ack_db_failed_cnt > 0):
''',
    '''    # Normal completion must use the same broker-truth contract as degraded returns.
    routed_sent_total, ack_cnt, reject_cnt, blocked_cnt = _routed_order_truth_counts(orders)
    dry_cnt = sum(1 for o in orders if o["status"] == "DRY_RUN")
    exit_closed_cnt = sum(1 for o in orders if o["status"] == "OK_EXIT_POSITION_CLOSED")
    sell_reconcile_pending_cnt = sum(1 for o in orders if o["status"] == "WARN_SELL_REJECT_RECONCILE_PENDING")
    signal_only_cnt = sum(1 for o in orders if o["status"] == "SIGNAL_ONLY")
    err_cnt = sum(1 for o in orders if o["status"] == "ERROR")
    ack_db_failed_cnt = sum(1 for o in orders if o.get("status") == "ACK_DB_FAILED")

    if not offline and ack_cnt > 0:
''',
    "US normal truth counts",
)
text = replace_once(
    text,
    '''            ack_recon_after_route = {"status": "ACK_PENDING_RECONCILE", "error": str(exc), "pending_count": ack_cnt + ack_db_failed_cnt, "confirmed_count": 0, "balance_reconcile_count": 0, "unresolved_count": ack_cnt + ack_db_failed_cnt, "symbols_by_status": {"ack_pending_reconcile": []}}
''',
    '''            ack_recon_after_route = {"status": "ACK_PENDING_RECONCILE", "error": str(exc), "pending_count": ack_cnt, "confirmed_count": 0, "balance_reconcile_count": 0, "unresolved_count": ack_cnt, "symbols_by_status": {"ack_pending_reconcile": []}}
''',
    "US reconcile fallback",
)
text = replace_once(
    text,
    '''    # ACK_DB_FAILED still means the broker accepted a real submission.
    orders_sent = ack_cnt + dry_cnt + ack_db_failed_cnt
''',
    '''    # Broker submission truth is authoritative across normal and degraded paths.
    orders_sent = routed_sent_total
''',
    "US final orders_sent",
)
us.write_text(text, encoding="utf-8")

kr = Path("trader/kr/runner/trade_session_runner.py")
ktext = kr.read_text(encoding="utf-8")
ktext = replace_once(
    ktext,
    '''    if session == "close" and pb1_last == "SKIP_PHASE_WINDOW":
        status = "FAIL"
        exit_code = max(exit_code, 2)
        summary_reason = "CLOSE_PHASE_NOT_EXECUTED"
        logger.error("[KR_CLOSE][FAIL] reason=CLOSE_PHASE_NOT_EXECUTED")
    if not summary_reason:
        summary_reason = "DB_EXACT_FINAL30_ZERO" if (pb1_last == "FAIL_PRECHECK" or "DB_EXACT_FINAL30_ZERO" in pb1_reason) else "PB1_SESSION_DONE"
    blocked = 0
    if session == "close" and balance_state is not None and balance_state.get("status") == "WARN":
        status = "WARN"
        summary_reason = "CLOSE_BALANCE_UNCONFIRMED"
        blocked = 1
        logger.warning("[KR_CLOSE][WARN] reason=BALANCE_UNCONFIRMED close_orders_blocked=1")
''',
    '''    close_phase_not_executed = session == "close" and pb1_last == "SKIP_PHASE_WINDOW"
    if close_phase_not_executed:
        status = "FAIL"
        exit_code = max(exit_code, 2)
        summary_reason = "CLOSE_PHASE_NOT_EXECUTED"
        logger.error("[KR_CLOSE][FAIL] reason=CLOSE_PHASE_NOT_EXECUTED")
    if not summary_reason:
        summary_reason = "DB_EXACT_FINAL30_ZERO" if (pb1_last == "FAIL_PRECHECK" or "DB_EXACT_FINAL30_ZERO" in pb1_reason) else "PB1_SESSION_DONE"
    blocked = 0
    if session == "close" and balance_state is not None and balance_state.get("status") == "WARN":
        blocked = 1
        if close_phase_not_executed:
            logger.error("[KR_CLOSE][FAIL_PRESERVED] reason=CLOSE_PHASE_NOT_EXECUTED balance_warning=BALANCE_UNCONFIRMED close_orders_blocked=1")
        else:
            status = "WARN"
            summary_reason = "CLOSE_BALANCE_UNCONFIRMED"
            logger.warning("[KR_CLOSE][WARN] reason=BALANCE_UNCONFIRMED close_orders_blocked=1")
''',
    "KR close failure precedence",
)
kr.write_text(ktext, encoding="utf-8")

ust = Path("tests/us/test_us_pr127_residual_integrity.py")
utext = ust.read_text(encoding="utf-8")
if "def test_normal_completion_reuses_broker_truth_counts" not in utext:
    utext += '''\n\ndef test_normal_completion_reuses_broker_truth_counts():
    source = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    assert "routed_sent_total, ack_cnt, reject_cnt, blocked_cnt = _routed_order_truth_counts(orders)" in source
    assert "orders_sent = routed_sent_total" in source
    assert '\"pending_count\": ack_cnt + ack_db_failed_cnt' not in source
'''
    ust.write_text(utext, encoding="utf-8")

krt = Path("tests/kr/test_kr_pr127_close_contract.py")
krtest = krt.read_text(encoding="utf-8")
if "def test_close_phase_failure_precedes_balance_warning" not in krtest:
    krtest += '''\n\ndef test_close_phase_failure_precedes_balance_warning():
    source = Path("trader/kr/runner/trade_session_runner.py").read_text(encoding="utf-8")
    assert 'close_phase_not_executed = session == "close" and pb1_last == "SKIP_PHASE_WINDOW"' in source
    close_guard = source.index('close_phase_not_executed = session == "close" and pb1_last == "SKIP_PHASE_WINDOW"')
    balance_guard = source.index('if session == "close" and balance_state is not None and balance_state.get("status") == "WARN":', close_guard)
    section = source[balance_guard:balance_guard + 750]
    assert 'if close_phase_not_executed:' in section
    assert '[KR_CLOSE][FAIL_PRESERVED]' in section
    assert section.index('if close_phase_not_executed:') < section.index('status = "WARN"')
'''
    krt.write_text(krtest, encoding="utf-8")
