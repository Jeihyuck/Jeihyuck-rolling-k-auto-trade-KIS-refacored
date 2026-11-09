"""2026-11-09 KR: local HASH is not independent broker execution evidence."""
from __future__ import annotations

from trader.db.repos import KR_BUY_ENTRY_CONTRACT_VERSION, _kr_buy_entry_contract_hash
from trader.kr.economic_chain_integrity import audit_kr_buy_identity

EPOCH = "epoch-demo"


def _order(code="293490", qty=174):
    request = {
        "entry_exit_plan": {"trade_horizon": "SWING", "exit_policy_family": "SWING_STAGED_EXIT"},
        "pre_order_holding_qty": 0,
        "requested_qty": qty,
        "submitted_qty": qty,
        "client_order_key": "demo:" + code,
        "position_cycle_id": "exact-" + code,
        "portfolio_epoch_id": "portfolio-demo",
        "trading_epoch_id": EPOCH,
        "entry_contract_version": KR_BUY_ENTRY_CONTRACT_VERSION,
    }
    request["entry_contract_sha256"] = _kr_buy_entry_contract_hash(request)
    return {
        "env": "practice", "order_id": "order-" + code,
        "code": code, "side": "BUY", "qty": qty,
        "status": "RECONCILED", "kis_odno": "0000011280",
        "trading_epoch_id": EPOCH, "portfolio_epoch_id": "portfolio-demo",
        "position_cycle_id": "exact-" + code,
        "request_json": request,
        "response_json": {
            "_kr_buy_entry_contract": {
                "entry_contract_sha256": request["entry_contract_sha256"],
            },
            "tot_ccld_qty": "0", "tot_ccld_amt": "0",
        },
    }


def _fill(order):
    return {
        "order_id": order["order_id"],
        "code": order["code"], "side": "BUY", "qty": order["qty"],
        "price": 11750, "kis_odno": order["kis_odno"],
        "trading_epoch_id": EPOCH,
        "portfolio_epoch_id": "portfolio-demo",
        "position_cycle_id": order["position_cycle_id"],
        "fill_meta_json": {
            "evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION",
            "fill_source": "daily_ccld_order_cumulative",
        },
    }


def _position(order, *, imported=False):
    return {
        "code": order["code"], "qty": order["qty"],
        "status": "OPEN", "trading_epoch_id": EPOCH, "portfolio_epoch_id": "portfolio-demo",
        "position_cycle_id": "imported-other-cycle" if imported else order["position_cycle_id"],
        "position_origin": "IMPORTED" if imported else "SYSTEM",
    }


def test_kakao_contract_hash_is_not_kis_fill_proof():
    order = _order()
    report = audit_kr_buy_identity(
        order=order, fills=[], positions=[_position(order, imported=True)],
        claim={"action_state": "UNCERTAIN"},
    )
    assert report["contract_sha_matches_local_echo"] is True
    assert report["stored_native_priced_buy_qty"] == 0
    assert report["status"] == "CHAIN_INCOMPLETE"
    assert "ORIGINAL_PRICED_BUY_FILL_MISSING" in report["issues"]
    assert "POSITION_CYCLE_NOT_BOUND_TO_BUY" in report["issues"]
    assert "IMPORTED_HOLDING_WITHOUT_EXACT_BUY_PROOF" in report["issues"]


def test_mico_priced_broker_response_and_satisfied_claim_do_not_replace_fill():
    order = _order("059090", 79)
    order["response_json"]["tot_ccld_qty"] = "79"
    order["response_json"]["tot_ccld_amt"] = "2057950"
    report = audit_kr_buy_identity(
        order=order, fills=[], positions=[_position(order, imported=True)],
        claim={"action_state": "SATISFIED"},
    )
    assert "BROKER_RESPONSE_EXECUTION_NOT_PERSISTED" in report["issues"]
    assert "SATISFIED_CLAIM_WITHOUT_PRICED_FILL" in report["issues"]
    assert report["status"] == "CHAIN_INCOMPLETE"


def test_exact_native_fill_and_position_cycle_pass():
    order = _order()
    report = audit_kr_buy_identity(
        order=order, fills=[_fill(order)], positions=[_position(order)],
        claim={"action_state": "SATISFIED"},
    )
    assert report["status"] == "CHAIN_EVIDENCE_PRESENT"
    assert report["issues"] == []
    assert report["stored_native_priced_buy_qty"] == order["qty"]


def test_other_order_fill_cannot_be_credited_by_same_symbol_or_hash():
    order = _order()
    wrong = {**_fill(order), "order_id": "other-order", "position_cycle_id": "other-cycle"}
    report = audit_kr_buy_identity(
        order=order, fills=[wrong], positions=[_position(order, imported=True)],
        claim={"action_state": "SATISFIED"},
    )
    assert report["stored_native_priced_buy_qty"] == 0
    assert "SATISFIED_CLAIM_WITHOUT_PRICED_FILL" in report["issues"]


def test_native_fill_with_wrong_epoch_is_not_applied():
    order = _order()
    wrong = {**_fill(order), "trading_epoch_id": "old-epoch"}
    report = audit_kr_buy_identity(
        order=order, fills=[wrong], positions=[_position(order)],
    )
    assert "FILL_IDENTITY_OR_CYCLE_MISMATCH" in report["issues"]
    assert "ORIGINAL_PRICED_BUY_FILL_MISSING" in report["issues"]


def test_orphaned_fill_without_position_cannot_pass_as_complete():
    order = _order()
    report = audit_kr_buy_identity(order=order, fills=[_fill(order)], positions=[],
                                    claim={"action_state": "SATISFIED"})
    assert report["status"] == "CHAIN_INCOMPLETE"
    assert "POSITION_CYCLE_NOT_BOUND_TO_BUY" in report["issues"]


def test_cross_portfolio_fill_is_never_credited():
    order = _order()
    wrong = {**_fill(order), "portfolio_epoch_id": "foreign-portfolio"}
    report = audit_kr_buy_identity(order=order, fills=[wrong],
                                    positions=[_position(order)],
                                    claim={"action_state": "SATISFIED"})
    assert report["status"] == "CHAIN_INCOMPLETE"
    assert "FILL_IDENTITY_OR_CYCLE_MISMATCH" in report["issues"]


def test_missing_kis_order_number_cannot_pass_as_native_proof():
    order = _order()
    order["kis_odno"] = None
    report = audit_kr_buy_identity(order=order, fills=[_fill(order)],
                                    positions=[_position(order)])
    assert report["status"] == "CHAIN_INCOMPLETE"
    assert "ORIGINAL_KIS_ORDER_NUMBER_UNPROVEN" in report["issues"]
