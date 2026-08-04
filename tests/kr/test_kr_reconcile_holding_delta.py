from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

from trader.reconcile_kis import _promote_open_buy_orders_from_holdings


def _open_buy_order(*, pre_holding: int | None, submitted_qty: int, qty: int = 1) -> dict:
    request_json = {"requested_qty": qty, "submitted_qty": submitted_qty}
    if pre_holding is not None:
        request_json["pre_order_holding_qty"] = pre_holding
    return {
        "order_id": "ord-1",
        "code": "090430",
        "side": "BUY",
        "status": "ACKED",
        "qty": qty,
        "limit_price": 10000.0,
        "stage": "PB1-CLOSE",
        "client_order_key": "practice:pb1:090430:BUY:1",
        "kis_odno": "ODNO-1",
        "request_json": request_json,
        "response_json": {"_order_execution": {"submitted_qty": submitted_qty, "requested_qty": qty}},
        "submitted_at": datetime(2026, 8, 4, 13, 0, 0),
        "acked_at": datetime(2026, 8, 4, 13, 0, 1),
    }


def test_reconcile_does_not_promote_when_holding_delta_is_zero() -> None:
    orders_repo = MagicMock()
    fills_repo = MagicMock()
    orders_repo.get_open_orders.return_value = [_open_buy_order(pre_holding=1, submitted_qty=1, qty=1)]
    holdings_rows = [{"pdno": "090430", "hldg_qty": "1", "pchs_avg_pric": "10000"}]

    result = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id="run-1",
        tick_ts=datetime(2026, 8, 4, 13, 1, 0),
        holdings_rows=holdings_rows,
        orders_repo=orders_repo,
        fills_repo=fills_repo,
    )

    assert result["orders"] == 1
    assert result["fills"] == 0
    fills_repo.upsert_fill.assert_not_called()
    status = orders_repo.upsert_reconciled_order.call_args.kwargs["status"]
    assert status == "ACKED"


def test_reconcile_partial_fill_uses_delta_capped_by_submitted_qty() -> None:
    orders_repo = MagicMock()
    fills_repo = MagicMock()
    orders_repo.get_open_orders.return_value = [_open_buy_order(pre_holding=1, submitted_qty=3, qty=3)]
    holdings_rows = [{"pdno": "090430", "hldg_qty": "2", "pchs_avg_pric": "10000"}]

    result = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id="run-1",
        tick_ts=datetime(2026, 8, 4, 13, 1, 0),
        holdings_rows=holdings_rows,
        orders_repo=orders_repo,
        fills_repo=fills_repo,
    )

    assert result["orders"] == 1
    assert result["fills"] == 1
    status = orders_repo.upsert_reconciled_order.call_args.kwargs["status"]
    assert status == "PARTIAL_FILLED"
    fill_qty = fills_repo.upsert_fill.call_args.kwargs["qty"]
    assert fill_qty == 1
