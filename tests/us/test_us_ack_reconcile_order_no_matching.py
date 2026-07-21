from trader.us.db import repos
from trader.us.execution.reconcile import reconcile_ack_orders_with_balance


class Provider:
    def get_balance(self, **kwargs):
        return {"positions": []}

    def get_fills_by_order_no(self, **kwargs):
        return {"status": "OK", "order_no": "31806", "symbol": "AMAT", "side": "SELL", "filled_qty": 1, "avg_price": 100}


def test_ack_reconcile_accepts_unpadded_kis_order_number():
    repos.reset_memory_stores()
    assert repos.save_order_ack({"client_order_key": "ACK", "symbol": "AMAT", "side": "SELL", "qty_requested": 1, "order_no": "0000031806", "status": "ACK"}, "2026-07-20")
    result = reconcile_ack_orders_with_balance(provider=Provider(), trade_date="2026-07-20")
    assert result["status"] == "OK"
    assert result["confirmed_count"] == 1
    assert repos.load_pending_ack_orders("2026-07-20") == []
