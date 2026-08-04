from __future__ import annotations

from trader.kis_wrapper import _attach_order_execution_meta


def test_order_execution_meta_for_sell_qty_adjustment() -> None:
    resp = {"rt_cd": "0", "msg_cd": "0", "msg1": "ok", "output": {"ODNO": "12345"}}
    out = _attach_order_execution_meta(
        resp,
        side="SELL",
        code="005830",
        requested_qty=6,
        submitted_qty=1,
        sellable_qty=1,
        kis_env="practice",
    )
    assert isinstance(out, dict)
    meta = out["_order_execution"]
    assert meta["requested_qty"] == 6
    assert meta["submitted_qty"] == 1
    assert meta["sellable_qty"] == 1
    assert meta["accepted"] is True
    assert meta["broker_order_id"] == "12345"
