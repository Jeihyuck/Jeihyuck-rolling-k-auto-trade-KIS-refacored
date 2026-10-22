"""Exact KIS-vs-ledger parity protects fast-path of historical repair.

Even a single missing/ambiguous policy, owner, position or KIS unit makes the
expensive existing repair run. These tests place no broker orders.
"""
from copy import deepcopy

import pytest

from trader.kr.reconcile_parity_fastpath import historical_repair_unnecessary


def good_data():
    holding = [{"pdno": "053800", "hldg_qty": "12", "pchs_avg_pric": "90200"},
               {"pdno": "122630", "hldg_qty": "85", "pchs_avg_pric": "34000"}]
    position = [{
        "code": "053800", "status": "OPEN", "strategy": "pb1_pullback_close",
        "qty": 12, "avg_buy_price": 90200,
        "position_cycle_id": "bb24815a-f713-49ab-8a05-bde33c9d8a53",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "entry_meta_json": {"entry_style_selected": "ENTRY_PULLBACK",
                            "exit_policy_family": "SWING_STAGED_EXIT"},
        "entry_exit_plan_json": {"exit_policy_family": "SWING_STAGED_EXIT"},
    }]
    return holding, position


def evaluate(holdings, positions, **changes):
    flags = dict(
        holdings=holdings, positions=positions, reconcile_ok=True,
        holdings_error=None, order_or_fill_changes=0,
        unresolved=False, expected_strategy="pb1_pullback_close",
    )
    flags.update(changes)
    return historical_repair_unnecessary(**flags)


def test_exact_current_owner_and_frozen_contract_skips_only_historical_repair():
    holdings, positions = good_data()
    assert evaluate(holdings, positions) == (True, "EXACT_ACTIVE_CYCLE_PARITY")


@pytest.mark.parametrize("override,reason", [
    ({"reconcile_ok": False}, "BROKER_EVIDENCE_UNRESOLVED"),
    ({"holdings_error": "KIS_500"}, "BROKER_EVIDENCE_UNRESOLVED"),
    ({"unresolved": True}, "NEW_OR_UNRESOLVED_EXECUTION"),
    ({"order_or_fill_changes": 1}, "NEW_OR_UNRESOLVED_EXECUTION"),
])
def test_any_external_broker_uncertainty_runs_old_full_recovery(override, reason):
    h,p=good_data()
    assert evaluate(h,p,**override) == (False,reason)


@pytest.mark.parametrize("field,bad,expected", [
    ("qty",13,"KIS_LEDGER_QUANTITY_MISMATCH"),
    ("avg_buy_price",0,"KIS_LEDGER_QUANTITY_MISMATCH"),
    ("position_cycle_id","","MISSING_POSITION_CYCLE"),
    ("strategy","KR_INFINITE","STRATEGY_OWNER_MISMATCH"),
    ("exit_policy_family",None,"MISSING_POLICY_FAMILY"),
    ("entry_exit_plan_json",{}, "MISSING_FROZEN_EXIT_CONTRACT"),
    ("entry_meta_json",None,"MISSING_FROZEN_EXIT_CONTRACT"),
])
def test_bad_policy_owner_cycle_or_quantity_requires_recovery(field,bad,expected):
    h,p=good_data()
    p[0][field]=bad
    if field=="exit_policy_family":
        p[0]["entry_meta_json"].pop("exit_policy_family",None)
    assert evaluate(h,p) == (False,expected)


def test_two_active_same_code_never_chooses_arbitrary_closed_policy():
    h,p=good_data()
    duplicate = deepcopy(p[0])
    duplicate["position_cycle_id"] = "old-momentum-cycle"
    duplicate["exit_policy_family"] = "DAY_TRADE"
    assert evaluate(h,p+[duplicate]) == (False,"AMBIGUOUS_ACTIVE_CYCLE")


def test_closed_historical_cycle_not_attributed_as_active():
    h,p=good_data()
    old=deepcopy(p[0]); old["status"]="CLOSED";old["qty"]=0
    assert evaluate(h,p+[old]) == (True,"EXACT_ACTIVE_CYCLE_PARITY")


def test_unknown_broker_holding_does_not_disappear_behind_infinite_reserved_sleeve():
    h,p=good_data()
    h.append({"pdno":"253590","hldg_qty":"79"})
    assert evaluate(h,p) == (False,"KIS_LEDGER_SYMBOL_MISMATCH")


def test_duplicate_kis_balance_rows_do_not_double_count_or_skip_repair():
    h,p=good_data()
    h.insert(1,deepcopy(h[0]))
    assert evaluate(h,p) == (False,"DUPLICATE_BROKER_HOLDING")
