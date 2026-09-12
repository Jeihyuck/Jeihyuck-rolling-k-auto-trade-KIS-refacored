from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if new in text:
        return
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected exactly one match, got {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# Use one broker-submission truth predicate for counters and routed notional.
replace_once(
    "trader/us/runner/trade_tick_runner.py",
    '''def _routed_order_truth_counts(orders: list[dict]) -> tuple[int, int, int, int]:\n    submitted_statuses = {\n        "ACK", "ACK_DB_FAILED", "ACK_DB_FAILED_RECONCILE_REQUIRED",\n        "BROKER_SUBMIT_RESULT_UNKNOWN", "ACK_JOURNAL_FAILED_RECONCILE_REQUIRED",\n        "DB_ACK_JOURNAL_FAILED_RECONCILE_REQUIRED", "REJECT", "REJECTED",\n        "SUBMITTED", "SENT", "DRY_RUN", "SIGNAL_ONLY",\n    }\n    ack_statuses = {\n        "ACK", "ACK_DB_FAILED", "ACK_DB_FAILED_RECONCILE_REQUIRED",\n        "ACK_JOURNAL_FAILED_RECONCILE_REQUIRED",\n        "DB_ACK_JOURNAL_FAILED_RECONCILE_REQUIRED", "FILLED",\n    }\n    rejected_statuses = {"REJECT", "REJECTED"}\n    blocked_statuses = {"BLOCKED", "WARN_DUPLICATE_EXIT_BLOCKED"}\n\n    sent = ack = rejected = blocked = 0\n    for order in orders or []:\n        if not isinstance(order, dict):\n            continue\n        status = str(order.get("status") or "").upper()\n        if bool(order.get("broker_submit")) or status in submitted_statuses:\n            sent += 1\n        if bool(order.get("kis_ack")) or status in ack_statuses:\n            ack += 1\n        if status in rejected_statuses:\n            rejected += 1\n        if status in blocked_statuses:\n            blocked += 1\n    return sent, ack, rejected, blocked\n''',
    '''_ROUTED_SUBMISSION_STATUSES = {\n    "ACK", "ACK_DB_FAILED", "ACK_DB_FAILED_RECONCILE_REQUIRED",\n    "BROKER_SUBMIT_RESULT_UNKNOWN", "ACK_JOURNAL_FAILED_RECONCILE_REQUIRED",\n    "DB_ACK_JOURNAL_FAILED_RECONCILE_REQUIRED", "SUBMITTED", "SENT",\n    "DRY_RUN", "SIGNAL_ONLY",\n}\n_ROUTED_ACK_STATUSES = {\n    "ACK", "ACK_DB_FAILED", "ACK_DB_FAILED_RECONCILE_REQUIRED",\n    "ACK_JOURNAL_FAILED_RECONCILE_REQUIRED",\n    "DB_ACK_JOURNAL_FAILED_RECONCILE_REQUIRED", "FILLED",\n}\n\n\ndef _order_has_routed_submission_truth(order: dict) -> bool:\n    if not isinstance(order, dict):\n        return False\n    status = str(order.get("status") or "").upper()\n    return bool(order.get("broker_submit")) or status in _ROUTED_SUBMISSION_STATUSES\n\n\ndef _order_has_ack_truth(order: dict) -> bool:\n    if not isinstance(order, dict):\n        return False\n    status = str(order.get("status") or "").upper()\n    return bool(order.get("kis_ack")) or status in _ROUTED_ACK_STATUSES\n\n\ndef _routed_order_truth_counts(orders: list[dict]) -> tuple[int, int, int, int]:\n    rejected_statuses = {"REJECT", "REJECTED"}\n    blocked_statuses = {"BLOCKED", "WARN_DUPLICATE_EXIT_BLOCKED"}\n\n    sent = ack = rejected = blocked = 0\n    for order in orders or []:\n        if not isinstance(order, dict):\n            continue\n        status = str(order.get("status") or "").upper()\n        if _order_has_routed_submission_truth(order):\n            sent += 1\n        if _order_has_ack_truth(order):\n            ack += 1\n        if status in rejected_statuses:\n            rejected += 1\n        if status in blocked_statuses:\n            blocked += 1\n    return sent, ack, rejected, blocked\n\n\ndef _routed_order_notional(orders: list[dict]) -> float:\n    total = 0.0\n    for order in orders or []:\n        if not _order_has_routed_submission_truth(order):\n            continue\n        intent = order.get("intent") if isinstance(order.get("intent"), dict) else {}\n        try:\n            total += float(intent.get("notional_usd", 0) or 0)\n        except (TypeError, ValueError):\n            continue\n    return total\n''',
)

replace_once(
    "trader/us/runner/trade_tick_runner.py",
    '''    ack = sum(1 for o in orders if o.get("status") == "ACK")\n    sent = sum(1 for o in orders if o.get("status") in {"ACK", "DRY_RUN", "SIGNAL_ONLY"})\n    rejected = sum(1 for o in orders if o.get("status") == "REJECT")\n    blocked = sum(1 for o in orders if o.get("status") in {"BLOCKED", "WARN_DUPLICATE_EXIT_BLOCKED"})\n    sell_notional_routed = sum(\n        float((o.get("intent") or {}).get("notional_usd", 0) or 0)\n        for o in orders\n        if o.get("status") in {"ACK", "DRY_RUN", "SIGNAL_ONLY"}\n    )\n''',
    '''    sent, ack, rejected, blocked = _routed_order_truth_counts(orders)\n    sell_notional_routed = _routed_order_notional(orders)\n''',
)


tests = Path("tests/us/test_us_pr127_residual_integrity.py")
text = tests.read_text(encoding="utf-8")
append = r'''


def test_routed_order_notional_uses_same_broker_submission_truth():
    from trader.us.runner.trade_tick_runner import _routed_order_notional

    orders = [
        {"status": "ACK_DB_FAILED", "broker_submit": True, "kis_ack": True,
         "intent": {"notional_usd": 100.0}},
        {"status": "ACK_JOURNAL_FAILED_RECONCILE_REQUIRED", "broker_submit": True, "kis_ack": True,
         "intent": {"notional_usd": 200.0}},
        {"status": "BROKER_SUBMIT_RESULT_UNKNOWN", "broker_submit": True, "kis_ack": True,
         "intent": {"notional_usd": 300.0}},
        {"status": "REJECT", "broker_submit": True, "kis_ack": False,
         "intent": {"notional_usd": 400.0}},
        {"status": "REJECT", "broker_submit": False, "kis_ack": False,
         "intent": {"notional_usd": 500.0}},
        {"status": "BLOCKED", "broker_submit": False, "kis_ack": False,
         "intent": {"notional_usd": 600.0}},
    ]
    # The first four were actually submitted to the broker.  A local/non-submitted
    # reject and a blocked order must not inflate routed SELL notional.
    assert _routed_order_notional(orders) == 1000.0
'''
if "test_routed_order_notional_uses_same_broker_submission_truth" not in text:
    tests.write_text(text + append, encoding="utf-8")
