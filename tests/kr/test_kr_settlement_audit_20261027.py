"""Offline regression for historical 2026-10-26/27 DB fill gaps."""
from trader.kr.settlement_audit import audit_persisted_fill_evidence


def _order(**kw):
    obj = {"order_id": "order1", "code": "293490", "side": "SELL",
           "qty": 170, "status": "FILLED", "position_cycle_id": "cycle1"}
    obj.update(kw)
    return obj


def _fill(qty, **kw):
    obj = {"order_id": "order1", "code": "293490", "side": "SELL",
           "qty": qty, "price": 11292.4, "position_cycle_id": "cycle1"}
    obj.update(kw)
    return obj


def test_oct26_partial_persisted_fill_not_silently_treated_as_full():
    proof = audit_persisted_fill_evidence(_order(), [_fill(17)])
    assert proof["db_fill_qty"] == 17
    assert proof["requested_qty"] == 170
    assert proof["fill_evidence"] == "PARTIAL_DB_FILL"


def test_oct27_filled_order_with_no_db_fill_is_audit_gap_not_broker_rejection():
    proof = audit_persisted_fill_evidence(_order(), [])
    assert proof["fill_evidence"] == "STATUS_WITHOUT_DB_FILL"
    assert proof["gross_proceeds_or_cost"] is None
    assert proof["risk"] == "AUDIT_ONLY"


def test_order_and_cycle_id_must_match():
    assert audit_persisted_fill_evidence(_order(), [_fill(17, order_id="order2")])["fill_evidence"] == "CROSS_ORDER_FILL_PROVENANCE"
    assert audit_persisted_fill_evidence(_order(), [_fill(17, position_cycle_id="cycle2")])["fill_evidence"] == "FILL_CYCLE_MISMATCH"
    assert audit_persisted_fill_evidence(_order(), [_fill(17, code="012750")])["fill_evidence"] == "FILL_OWNER_MISMATCH"


def test_full_persisted_fill_and_nonfinite_proceeds():
    proven = audit_persisted_fill_evidence(_order(), [_fill(70), _fill(100)])
    assert proven["fill_evidence"] == "FULL_DB_FILL_PROVEN"
    assert proven["db_fill_qty"] == 170
    bad = audit_persisted_fill_evidence(_order(), [_fill(170, price=float("nan"))])
    assert bad["fill_evidence"] == "INVALID_DB_FILL_PROOF"
