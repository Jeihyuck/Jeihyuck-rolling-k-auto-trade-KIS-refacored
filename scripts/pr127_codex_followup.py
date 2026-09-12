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


# P1-1: Query failures after a prior TQQQ cancel request must still age-escalate
# to durable manual reconciliation without inventing a terminal broker state.
replace_once(
    "trader/us/infinite/integration.py",
    '''        try:\n            observation = query_order(**identity) or {}\n        except Exception as exc:\n            logger.warning(\n                "[TQQQ_INF][TTL_RECONCILE][QUERY_WARN] order_no=%s key=%s error=%s action=keep_pending",\n                order_no, client_order_key, exc,\n            )\n            result["pending"] += 1\n            continue\n''',
    '''        try:\n            observation = query_order(**identity) or {}\n        except Exception as exc:\n            logger.warning(\n                "[TQQQ_INF][TTL_RECONCILE][QUERY_WARN] order_no=%s key=%s error=%s action=keep_pending",\n                order_no, client_order_key, exc,\n            )\n            # A sustained broker-query outage must not bypass the liveness\n            # escalation added for prior cancel requests.  Keep the order\n            # non-terminal/fail-closed, but persist RED/manual reconciliation\n            # once the unresolved cancel has exceeded its escalation age.\n            cancel_requested_at = metadata.get("tqqq_ttl_cancel_requested_at")\n            if cancel_requested_at:\n                unresolved_age_sec = 0.0\n                try:\n                    requested_dt = datetime.fromisoformat(str(cancel_requested_at).replace("Z", "+00:00"))\n                    if requested_dt.tzinfo is None:\n                        requested_dt = requested_dt.replace(tzinfo=timezone.utc)\n                    unresolved_age_sec = max(\n                        0.0,\n                        (now.astimezone(timezone.utc) - requested_dt.astimezone(timezone.utc)).total_seconds(),\n                    )\n                except Exception:\n                    unresolved_age_sec = 0.0\n                escalate_after_sec = max(\n                    int(ttl_seconds),\n                    int(os.getenv("US_TQQQ_TTL_UNRESOLVED_ESCALATE_SEC", "900") or 900),\n                )\n                if unresolved_age_sec >= escalate_after_sec:\n                    result["escalated"] = int(result.get("escalated", 0)) + 1\n                    if not metadata.get("tqqq_ttl_unresolved_escalated_at"):\n                        logger.error(\n                            "[TQQQ_INF][TTL_RECONCILE][ESCALATED] order_no=%s key=%s unresolved_age_sec=%.1f action=manual_reconcile_required buy_fence=keep trigger=query_error",\n                            order_no, client_order_key, unresolved_age_sec,\n                        )\n                        if hasattr(repository, "mark_ttl_unresolved_escalated"):\n                            repository.mark_ttl_unresolved_escalated(\n                                order, escalated_at=now, unresolved_age_sec=unresolved_age_sec,\n                            )\n            result["pending"] += 1\n            continue\n''',
)


# P1-2: Centralize already-routed order truth.  Broker submission / KIS ACK
# flags are authoritative even when durable DB/journal persistence changes the
# outward status string.
replace_once(
    "trader/us/runner/trade_tick_runner.py",
    '''    return passthrough + merged\n\n\ndef route_exit_orders_immediately(\n''',
    '''    return passthrough + merged\n\n\ndef _routed_order_truth_counts(orders: list[dict]) -> tuple[int, int, int, int]:\n    submitted_statuses = {\n        "ACK", "ACK_DB_FAILED", "ACK_DB_FAILED_RECONCILE_REQUIRED",\n        "BROKER_SUBMIT_RESULT_UNKNOWN", "ACK_JOURNAL_FAILED_RECONCILE_REQUIRED",\n        "DB_ACK_JOURNAL_FAILED_RECONCILE_REQUIRED", "REJECT", "REJECTED",\n        "SUBMITTED", "SENT", "DRY_RUN", "SIGNAL_ONLY",\n    }\n    ack_statuses = {\n        "ACK", "ACK_DB_FAILED", "ACK_DB_FAILED_RECONCILE_REQUIRED",\n        "ACK_JOURNAL_FAILED_RECONCILE_REQUIRED",\n        "DB_ACK_JOURNAL_FAILED_RECONCILE_REQUIRED", "FILLED",\n    }\n    rejected_statuses = {"REJECT", "REJECTED"}\n    blocked_statuses = {"BLOCKED", "WARN_DUPLICATE_EXIT_BLOCKED"}\n\n    sent = ack = rejected = blocked = 0\n    for order in orders or []:\n        if not isinstance(order, dict):\n            continue\n        status = str(order.get("status") or "").upper()\n        if bool(order.get("broker_submit")) or status in submitted_statuses:\n            sent += 1\n        if bool(order.get("kis_ack")) or status in ack_statuses:\n            ack += 1\n        if status in rejected_statuses:\n            rejected += 1\n        if status in blocked_statuses:\n            blocked += 1\n    return sent, ack, rejected, blocked\n\n\ndef route_exit_orders_immediately(\n''',
)

replace_once(
    "trader/us/runner/trade_tick_runner.py",
    '''    routed_sent = sum(1 for order in orders if str(order.get("status") or "").upper() in {"ACK", "DRY_RUN", "SIGNAL_ONLY", "SUBMITTED", "SENT"})\n    routed_ack = sum(1 for order in orders if str(order.get("status") or "").upper() in {"ACK", "FILLED"})\n    routed_rejected = sum(1 for order in orders if str(order.get("status") or "").upper() in {"REJECT", "REJECTED"})\n    routed_blocked = sum(1 for order in orders if str(order.get("status") or "").upper() in {"BLOCKED", "WARN_DUPLICATE_EXIT_BLOCKED"})\n''',
    '''    routed_sent, routed_ack, routed_rejected, routed_blocked = _routed_order_truth_counts(orders)\n''',
)


tests = Path("tests/us/test_us_pr127_residual_integrity.py")
text = tests.read_text(encoding="utf-8")
append = r'''


def test_tqqq_query_exception_still_escalates_prior_cancel(monkeypatch):
    from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl

    now = datetime(2026, 9, 12, 2, 0, tzinfo=timezone.utc)
    order = {
        "trade_date": "2026-09-10",
        "order_no": "123",
        "client_order_key": "TQQQ_INF_V3:cycle:2026-09-10:BUY:1",
        "symbol": "TQQQ", "side": "BUY", "status": "ACK", "qty": 3,
        "meta": {"tqqq_ttl_cancel_requested_at": (now - timedelta(hours=1)).isoformat()},
    }

    class Repo:
        def __init__(self):
            self.escalations = []
        def load_expired_open_buy_orders(self, **kwargs):
            return [order]
        def mark_ttl_unresolved_escalated(self, order, **kwargs):
            self.escalations.append(kwargs)

    repo = Repo()
    monkeypatch.setenv("US_TQQQ_TTL_UNRESOLVED_ESCALATE_SEC", "900")
    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=now, ttl_seconds=120,
        cancel_order=lambda **kwargs: (_ for _ in ()).throw(AssertionError("cancel must not replay")),
        query_order=lambda **kwargs: (_ for _ in ()).throw(TimeoutError("sustained KIS query timeout")),
    )
    assert result["pending"] == 1
    assert result["terminal"] == 0
    assert result.get("escalated") == 1
    assert len(repo.escalations) == 1


def test_routed_order_truth_counts_broker_submission_and_ack_flags():
    from trader.us.runner.trade_tick_runner import _routed_order_truth_counts

    orders = [
        {"status": "ACK_DB_FAILED", "broker_submit": True, "kis_ack": True},
        {"status": "ACK_JOURNAL_FAILED_RECONCILE_REQUIRED", "broker_submit": True, "kis_ack": True},
        {"status": "BROKER_SUBMIT_RESULT_UNKNOWN", "broker_submit": True, "kis_ack": True},
        {"status": "REJECT", "broker_submit": True, "kis_ack": False},
        {"status": "BLOCKED", "broker_submit": False, "kis_ack": False},
    ]
    sent, ack, rejected, blocked = _routed_order_truth_counts(orders)
    assert sent == 4
    assert ack == 3
    assert rejected == 1
    assert blocked == 1
'''
if "test_tqqq_query_exception_still_escalates_prior_cancel" not in text:
    tests.write_text(text + append, encoding="utf-8")
