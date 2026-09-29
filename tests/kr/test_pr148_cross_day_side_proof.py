from __future__ import annotations

from datetime import datetime, timezone

from trader.kr import runtime_integrity_20260929 as base
from trader.kr.runtime_integrity_20260929_side_proof import (
    _explicit_buy_side,
    install_kr_20260929_side_proof_guard,
)


class _Kis:
    def __init__(self, row):
        self.row = dict(row)

    def inquire_daily_ccld(self, **_kwargs):
        return {"rt_cd": "0", "output1": [dict(self.row)]}


def _order():
    return {
        "created_at": datetime(2026, 9, 28, 0, 33, 4, tzinfo=timezone.utc),
        "kis_odno": "0000010034",
        "broker_order_id": "0000010034",
        "code": "028050",
        "qty": 21,
    }


def _row(**extra):
    row = {
        "odno": "0000010034",
        "pdno": "028050",
        "ord_qty": "21",
        "tot_ccld_qty": "21",
        "avg_prvs": "49350",
    }
    row.update(extra)
    return row


def test_explicit_buy_side_requires_positive_buy_evidence():
    assert _explicit_buy_side({}) is False
    assert _explicit_buy_side({"sll_buy_dvsn_cd": ""}) is False
    assert _explicit_buy_side({"sll_buy_dvsn_cd": "UNKNOWN"}) is False
    assert _explicit_buy_side({"sll_buy_dvsn_cd": "01"}) is False
    assert _explicit_buy_side({"side": "SELL"}) is False
    assert _explicit_buy_side({"sll_buy_dvsn_name": "매도"}) is False

    assert _explicit_buy_side({"sll_buy_dvsn_cd": "02"}) is True
    assert _explicit_buy_side({"side": "BUY"}) is True
    assert _explicit_buy_side({"sll_buy_dvsn_name": "매수"}) is True


def test_cross_day_execution_proof_rejects_missing_or_unknown_side():
    install_kr_20260929_side_proof_guard()

    assert base._execution_proof_for_order(_Kis(_row()), _order()) is None
    assert base._execution_proof_for_order(
        _Kis(_row(sll_buy_dvsn_cd="UNKNOWN")), _order()
    ) is None
    assert base._execution_proof_for_order(
        _Kis(_row(sll_buy_dvsn_cd="01")), _order()
    ) is None


def test_cross_day_execution_proof_accepts_explicit_buy_side_only():
    install_kr_20260929_side_proof_guard()

    proof = base._execution_proof_for_order(
        _Kis(_row(sll_buy_dvsn_cd="02")), _order()
    )
    assert proof is not None
    assert proof["filled_qty"] == 21
    assert proof["avg_price"] == 49350.0
    assert proof["odno"] == "0000010034"
