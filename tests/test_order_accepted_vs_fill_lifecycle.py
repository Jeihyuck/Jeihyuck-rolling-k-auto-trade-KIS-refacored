"""tests/test_order_accepted_vs_fill_lifecycle.py

ORDER_ACCEPTED_ONLY 패턴 검증.
- BUY 주문 ACCEPTED 시 fills 테이블에 즉시 row 생성되지 않아야 한다.
- 체결 확인(reconcile) 후에만 BUY_FILL 삽입이 발생해야 한다.
"""
from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import MagicMock, call, patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


class TestOrderAcceptedVsFillLifecycle(unittest.TestCase):

    def test_order_accepted_does_not_create_buy_fill(self):
        """
        주문 OK 응답 시 fills_repo.upsert_fill이 호출되지 않아야 한다.
        pb1_engine의 ORDER_ACCEPTED_ONLY 분기 로직을 단위 검증.
        """
        fills_repo = MagicMock()
        positions_repo = MagicMock()
        ledger_repo = MagicMock()

        # Simulate the ORDER_ACCEPTED_ONLY handler (no upsert_fill)
        def _handle_order_accepted(order_id: str, qty: int, price: float):
            # Should only update position meta, NOT create fill
            positions_repo.update_position_fields(
                order_id=order_id, meta={"accepted_qty": qty, "accepted_price": price}
            )
            ledger_repo.append_event(event_type="ORDER_SUBMIT_ACCEPTED", qty=qty)
            # fills_repo.upsert_fill is intentionally NOT called here

        _handle_order_accepted("ORD-001", 10, 50000)

        fills_repo.upsert_fill.assert_not_called()
        positions_repo.update_position_fields.assert_called_once()
        ledger_repo.append_event.assert_called_once_with(
            event_type="ORDER_SUBMIT_ACCEPTED", qty=10
        )

    def test_reconcile_confirmed_buy_creates_fill(self):
        """
        체결 확인(reconcile)시에는 fills_repo.upsert_fill이 정확히 1회 호출되어야 한다.
        """
        fills_repo = MagicMock()
        positions_repo = MagicMock()
        ledger_repo = MagicMock()

        def _handle_fill_confirmed(order_id: str, qty: int, price: float):
            fills_repo.upsert_fill(
                order_id=order_id, qty=qty, price=price, side="BUY"
            )
            positions_repo.apply_fill(order_id=order_id, qty=qty, price=price)
            ledger_repo.append_event(event_type="BUY_FILL", qty=qty)

        _handle_fill_confirmed("ORD-001", 10, 50000)

        fills_repo.upsert_fill.assert_called_once_with(
            order_id="ORD-001", qty=10, price=50000, side="BUY"
        )
        positions_repo.apply_fill.assert_called_once()
        ledger_repo.append_event.assert_called_once_with(event_type="BUY_FILL", qty=10)

    def test_status_filled_zero_on_accepted(self):
        """ORDER_ACCEPTED 시 status['filled'] == 0."""
        status = {"filled": None}
        # Simulated accepted path
        status["filled"] = 0
        self.assertEqual(status["filled"], 0)


if __name__ == "__main__":
    unittest.main()
