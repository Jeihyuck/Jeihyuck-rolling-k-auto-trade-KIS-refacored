from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.reconcile_kis import _promote_open_buy_orders_from_holdings


def test_promote_open_buy_order_from_kis_holdings() -> None:
    orders_repo = MagicMock()
    fills_repo = MagicMock()
    orders_repo.get_open_orders.return_value = [
        {
            "order_id": "ord-1",
            "env": "practice",
            "strategy": "pb1_pullback_close",
            "code": "067310",
            "side": "BUY",
            "status": "ACCEPTED",
            "qty": 3,
            "limit_price": 12345.0,
            "stage": "PB1-CLOSE",
            "client_order_key": "practice:pb1:067310:BUY:1",
            "kis_odno": "1001",
            "submitted_at": datetime(2026, 5, 1, 9, 1, 0),
            "acked_at": datetime(2026, 5, 1, 9, 1, 1),
        }
    ]
    holdings_rows = [
        {"pdno": "067310", "hldg_qty": "3", "pchs_avg_pric": "12345"}
    ]

    promoted = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id="run-1",
        tick_ts=datetime(2026, 5, 1, 9, 2, 0),
        holdings_rows=holdings_rows,
        orders_repo=orders_repo,
        fills_repo=fills_repo,
    )

    assert promoted == {"orders": 1, "fills": 1}
    orders_repo.upsert_reconciled_order.assert_called_once()
    fills_repo.upsert_fill.assert_called_once()
    order_kwargs = orders_repo.upsert_reconciled_order.call_args.kwargs
    fill_kwargs = fills_repo.upsert_fill.call_args.kwargs
    assert order_kwargs["status"] == "FILLED"
    assert order_kwargs["code"] == "067310"
    assert fill_kwargs["trade_id"] == "PROMOTE:1001"
    assert fill_kwargs["qty"] == 3


def test_promote_open_buy_order_skips_when_holdings_absent() -> None:
    orders_repo = MagicMock()
    fills_repo = MagicMock()
    orders_repo.get_open_orders.return_value = [
        {
            "code": "067310",
            "side": "BUY",
            "status": "ACCEPTED",
            "qty": 3,
        }
    ]

    promoted = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id="run-1",
        tick_ts=datetime(2026, 5, 1, 9, 2, 0),
        holdings_rows=[],
        orders_repo=orders_repo,
        fills_repo=fills_repo,
    )

    assert promoted == {"orders": 0, "fills": 0}
    orders_repo.upsert_reconciled_order.assert_not_called()
    fills_repo.upsert_fill.assert_not_called()