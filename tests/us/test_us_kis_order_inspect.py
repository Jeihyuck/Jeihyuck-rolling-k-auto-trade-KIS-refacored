"""Offline, no-KIS, no-DB-write coverage of the operational inquiry gate."""
import pytest

from scripts.us_kis_order_inspect import (
    classify_target,
    parse_targets,
    validate_trade_date,
)


def _order():
    return {
        "symbol": "MRVL", "order_no": "0000034237",
        "side": "SELL", "status": "OPEN",
        "qty_requested": 2, "qty_filled": 0,
        "claim_state": "IN_FLIGHT",
        "meta": {
            "strategy_owner": "US_STANDARD", "profit_capture_stage": "tp2",
            "position_lifecycle_id": "cycle-1",
        },
    }


def _broker(**updates):
    row = {
        "symbol": "MRVL", "order_no": "34237", "side": "SELL",
        "status": "OPEN", "requested_qty": 2, "filled_qty": 0,
        "remaining_qty": 2, "remaining_qty_present": True,
        "filled_qty_present": True, "normalization_result": "normalized",
    }
    row.update(updates)
    return row


def _decision(order=None, broker=None):
    return classify_target("MRVL", "34237", [_order()] if order is None else order,
                           [_broker()] if broker is None else broker)["decision"]


def test_verified_open_is_only_a_review_candidate():
    assert _decision() == "BROKER_OPEN_CANCELLATION_REVIEW"


def test_terminal_requires_explicit_zero_remaining_and_fill_count():
    assert _decision(broker=[_broker(status="CANCELLED", remaining_qty=0)]) == (
        "BROKER_TERMINAL_RECONCILIATION_REVIEW"
    )
    assert _decision(broker=[_broker(status="FILLED", filled_qty=2, remaining_qty=0)]) == (
        "BROKER_TERMINAL_RECONCILIATION_REVIEW"
    )
    assert _decision(broker=[_broker(status="CANCELLED", filled_qty_present=False,
                                   remaining_qty=0)]) == "FENCED_UNVERIFIED"


@pytest.mark.parametrize("db,broker,expected", [
    ([], [_broker()], "DB_ORDER_NOT_UNIQUE_OR_MISSING"),
    ([_order(), _order()], [_broker()], "DB_ORDER_NOT_UNIQUE_OR_MISSING"),
    ([_order()], [], "BROKER_NOT_FOUND_NO_PROOF"),
    ([_order()], [_broker(), _broker()], "BROKER_DUPLICATE_AMBIGUOUS"),
    ([_order()], [_broker(remaining_qty_present=False)], "BROKER_EVIDENCE_INCOMPLETE"),
    ([_order()], [_broker(normalization_result="quarantined")], "FENCED_UNVERIFIED"),
    ([_order()], [_broker(order_no="1234")], "BROKER_IDENTITY_MISMATCH"),
])
def test_ambiguous_or_missing_evidence_always_fences(db, broker, expected):
    assert _decision(db, broker) == expected


def test_other_strategy_and_missing_lifecycle_fenced():
    for meta in [
        {"strategy_owner": "TQQQ_INFINITE", "position_lifecycle_id": "cycle-1",
         "profit_capture_stage": "tp2"},
        {"strategy_owner": "US_STANDARD", "position_lifecycle_id": "",
         "profit_capture_stage": "tp2"},
    ]:
        order = _order()
        order["meta"] = meta
        assert _decision(order=[order]) == "OWNER_OR_LIFECYCLE_MISMATCH"


def test_parser_rejects_injection_and_duplicate_orders():
    assert parse_targets("MRVL:0000034237,AMD:35281") == [
        ("MRVL", "34237"), ("AMD", "35281")
    ]
    for invalid in ["MRVL:34237; echo FAIL", "MRVL:34237,MRVL:0000034237",
                    "MRVL:", "MRVL:34237,", "tqqq:1"]:
        with pytest.raises(ValueError):
            parse_targets(invalid)
    with pytest.raises(ValueError):
        validate_trade_date("2026-10-06;touch /tmp/foo")
