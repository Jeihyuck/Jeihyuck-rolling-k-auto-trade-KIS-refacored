"""Regression: eight US sell receipts are not eight verified strategy decisions."""
from __future__ import annotations

from trader.us.runner.order_chain_verification import audit_us_order_chains


def _fixture(n=8):
    orders, fills, journal = [], [], []
    for i in range(n):
        symbol = f"TEST{i}"
        key = f"source-order-{i}"
        attempt = f"attempt-{i}"
        order_no = f"{90000+i:010d}"
        qty = i + 1
        orders.append({
            "symbol": symbol, "side": "SELL",
            "strategy_owner": "US_STANDARD",
            "client_order_key": key, "submit_attempt_id": attempt,
            "raw_order_no": order_no, "filled_qty": qty,
            "strategy_reason": "TAKE_PROFIT_TP1",
            "decision_evidence_status": "MISSING",
            "risk_gate_result": None,
        })
        fills.append({
            "symbol": symbol, "side": "SELL", "order_no": str(int(order_no)),
            "client_order_key": key, "qty": qty,
            "meta": {"fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL"},
        })
        session = "am" if i < 6 else "afternoon"
        for event in (
            "BROKER_CLAIM_PREPARED", "BROKER_SUBMIT_STARTED",
            "BROKER_ACK_RECEIVED", "DB_ACK_PERSISTED",
            "JOURNAL_REPLAY_FILL_CONFIRMED", "ORDER_FILLED",
        ):
            journal.append({
                "symbol": symbol, "side": "SELL",
                "event_type": event, "client_order_key": key,
                "submit_attempt_id": attempt, "session": session,
            })
    # First attempted TP order failed *before* broker HTTP. A later _R
    # action with a different client key is one of the eight actual orders.
    for i in range(2):
        for event in ("BROKER_SUBMIT_STARTED", "BROKER_SUBMIT_ABORTED_PRE_IO"):
            journal.append({
                "symbol": f"TEST{6+i}", "side": "SELL",
                "event_type": event, "client_order_key": f"blocked-{i}",
                "submit_attempt_id": f"blocked-attempt-{i}",
                "session": "afternoon",
            })
    return orders, fills, journal


def test_us_nov2_eight_confirmed_broker_orders_are_policy_pending_without_5w1h():
    result = audit_us_order_chains(*_fixture())
    assert result["order_count"] == 8
    assert result["kis_broker_chain_verified_count"] == 8
    assert result["safe_pre_http_abort_attempt_count"] == 2
    assert result["broker_ack_by_session"] == {"am": 6, "afternoon": 2}
    assert result["missing_5w1h_count"] == 8
    assert result["policy_verdict_counts"] == {"PENDING_EVIDENCE": 8}
    assert result["status"] == "AUDIT_INCOMPLETE"
    assert all(o["broker_chain_status"] == "KIS_FILL_DB_JOURNAL_MATCHED"
               for o in result["orders"])
    assert not any(o["policy_verdict"] == "EXPECTED" for o in result["orders"])


def test_cumulative_qty_invariant_mismatch_is_flagged_not_fabricated():
    orders, fills, journal = _fixture(n=1)
    fills[0]["qty"] = 99
    result = audit_us_order_chains(orders, fills, journal)
    assert result["kis_broker_chain_verified_count"] == 0
    assert result["orders"][0]["broker_chain_status"] == "BROKER_DB_FILLED_QTY_MISMATCH"
    assert result["orders"][0]["policy_verdict"] == "PENDING_EVIDENCE"
    assert result["broker_db_quantity_conflict_count"] == 1
    assert result["orders"][0]["policy_verdict_reason"] == (
        "KIS_AND_DB_QUANTITY_CONTRADICTION_UNVERIFIED_CAUSE"
    )


def test_synthetic_confirmation_is_not_kis_execution():
    orders, fills, journal = _fixture(n=1)
    fills[0]["meta"]["fill_evidence_type"] = "BALANCE_DELTA_SYNTHETIC"
    result = audit_us_order_chains(orders, fills, journal)
    assert result["kis_broker_chain_verified_count"] == 0
    assert result["orders"][0]["kis_actual_fill_qty"] is None
    assert result["orders"][0]["policy_verdict"] == "PENDING_EVIDENCE"


def test_distinct_execution_ids_are_deduplicated_not_counted_as_cumulative_plus_execution():
    orders, fills, journal = _fixture(n=1)
    fills.clear()
    for qty, execution_id in ((1, "fill-1"), (1, "fill-1"), (2, "fill-2")):
        fills.append({
            "symbol": "TEST0", "side": "SELL", "order_no": "90000",
            "client_order_key": "source-order-0", "qty": qty,
            "fill_idempotency_key": execution_id,
            "meta": {"fill_evidence_type": "KIS_EXECUTION_ACTUAL"},
        })
    orders[0]["filled_qty"] = 3
    result = audit_us_order_chains(orders, fills, journal)
    assert result["orders"][0]["kis_actual_fill_qty"] == 3
    assert result["kis_broker_chain_verified_count"] == 1


def test_ambiguous_or_missing_broker_claim_cannot_claim_expected():
    orders, fills, journal = _fixture(n=1)
    journal = [e for e in journal if e["event_type"] != "BROKER_ACK_RECEIVED"]
    result = audit_us_order_chains(orders, fills, journal)
    assert result["orders"][0]["broker_chain_status"] == "ORDER_CHAIN_INCOMPLETE"
    assert result["orders"][0]["policy_verdict"] == "PENDING_EVIDENCE"
