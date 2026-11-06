"""Offline no-broker fixtures for current-account 2026-10-27 missing fill proof."""
from copy import deepcopy

import pytest

from scripts.kr_20261027_293490_readonly_proof import classify_kis_ccld


def original(*, cumulative=174, amount=2_044_500, average=11750):
    return {
        "ord_dt": "20261027", "pdno": "293490", "sll_buy_dvsn_cd": "02",
        "odno": "0000011280", "ord_qty": "174",
        "tot_ccld_qty": str(cumulative),
        "tot_ccld_amt": str(amount), "avg_prvs": str(average),
    }


def test_practice_kis_original_full_174_has_separate_truth_and_no_db_repair():
    result = classify_kis_ccld([original()], odno="0000011280")
    assert result["status"] == "KIS_FULL_FILL_PROVEN"
    assert result["confirmed_filled_qty"] == 174
    assert result["confirmed_avg_price"] == 11750
    assert result["broker_cumulative_proven"] is True


def test_partial_and_zero_fill_remain_distinct():
    partial = classify_kis_ccld(
        [original(cumulative=80, amount=940000)], odno="0000011280"
    )
    assert partial["status"] == "KIS_PARTIAL_FILL_PROVEN"
    assert partial["confirmed_filled_qty"] == 80
    zero = classify_kis_ccld(
        [original(cumulative=0, amount=0, average=0)], odno="0000011280"
    )
    assert zero["status"] == "KIS_NO_FILL_OBSERVED"
    assert zero["confirmed_filled_qty"] == 0


@pytest.mark.parametrize("change", [
    lambda r: r.update(ord_dt="20261026"),
    lambda r: r.update(pdno="122630"),
    lambda r: r.update(sll_buy_dvsn_cd="01"),
    lambda r: r.update(odno="0000011281"),
])
def test_never_borrow_older_or_foreign_order_proof(change):
    row = original()
    change(row)
    out = classify_kis_ccld([row], odno="0000011280")
    assert out["status"] == "KIS_EXACT_ORDER_MISSING_OR_AMBIGUOUS"
    assert not out["broker_cumulative_proven"]


def test_duplicate_original_row_is_not_double_counted_or_accepted():
    row = original()
    out = classify_kis_ccld([row, deepcopy(row)], odno="0000011280")
    assert out["matches"] == 2
    assert not out["broker_cumulative_proven"]


@pytest.mark.parametrize("change", [
    lambda r: r.update(ord_qty="173"),
    lambda r: r.update(tot_ccld_qty="175"),
    lambda r: r.update(tot_ccld_qty="-1"),
])
def test_quantity_conflict_is_never_success(change):
    row = original()
    change(row)
    out = classify_kis_ccld([row], odno="0000011280")
    assert out["status"] == "KIS_ORDER_QUANTITY_CONFLICT"
    assert not out["broker_cumulative_proven"]


@pytest.mark.parametrize("change", [
    lambda r: r.update(tot_ccld_amt="0"),
    lambda r: r.update(avg_prvs="0"),
    lambda r: r.update(avg_prvs="20000"),
    lambda r: r.update(tot_ccld_amt="nan"),
])
def test_no_limit_price_or_ack_can_replace_real_economic_evidence(change):
    row = original()
    change(row)
    out = classify_kis_ccld([row], odno="0000011280")
    assert out["status"] == "KIS_ECONOMIC_FILL_UNVERIFIED"
    assert not out["broker_cumulative_proven"]


def test_diagnostic_scope_is_explicitly_read_only():
    import inspect
    from scripts import kr_20261027_293490_readonly_proof as mod
    source = inspect.getsource(mod)
    for forbidden in ("place_order(", "order_cash(", "cancel_order(",
                      "reconcile_today(", "reconcile_kis(",
                      "mark_submitted(", "conn.commit("):
        assert forbidden not in source
