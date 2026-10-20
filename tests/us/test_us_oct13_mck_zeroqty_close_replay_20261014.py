"""2026-10-13 MCK real broker SELL 1 share, close classified qty=0."""
from trader.us.execution.reconcile import classify_ack_orders_with_final_balance


class _CloseProvider:
    def __init__(self, qty=1):
        self.qty = qty

    def get_balance(self, force_refresh=False):
        return {
            "balance_parse_status": "OK",
            "balance_complete": True,
            "balance_authoritative": True,
            "positions": [
                {"symbol": "MCK", "qty": self.qty, "orderable_qty": self.qty}
            ],
        }


def _mck():
    return {
        "side": "SELL", "symbol": "MCK",
        "order_no": "0000043251",
        "client_order_key": "US_PC_2026-10-13_MCK_lifecycle_TAKE_PROFIT_TP1",
        "submit_attempt_id": "75f828a9-218f-4725-b88d-f6ec911aa4f1",
        "pre_order_holding_qty": 2,
        "_sources": ["journal", "claim"],
        "active_claim": True,
    }


def test_mck_unique_real_sell_can_recover_missing_qty_from_close_delta():
    result = classify_ack_orders_with_final_balance(
        provider=_CloseProvider(), trade_date="2026-10-13",
        orders=[_mck()],
    )
    order = result["orders"][0]
    assert result["status"] == "DEGRADED_ACK_UNRESOLVED"
    assert result["pending_order_count"] == 1
    assert order["qty"] == 1
    assert order["final_position_qty"] == 1
    assert order["balance_delta_confirmed"] is True
    assert order["qty_recovered_from_unique_close_balance_delta"] is True
    assert order["final_status"] == "balance_delta_confirmed"


def test_two_same_symbol_attempts_never_infer_from_one_balance_delta():
    first = _mck()
    second = {**_mck(), "order_no": "0000043252",
              "submit_attempt_id": "different-attempt", "client_order_key": "different-key"}
    result = classify_ack_orders_with_final_balance(
        provider=_CloseProvider(), trade_date="2026-10-13",
        orders=[first, second],
    )
    assert result["pending_order_count"] > 0
    assert not any(order.get("qty_recovered_from_unique_close_balance_delta")
                   for order in result["orders"])


def test_no_claim_or_no_broker_order_identity_never_recovers_quantity():
    for change in (
        {"_sources": ["journal"]},
        {"order_no": ""},
        {"submit_attempt_id": ""},
        {"client_order_key": ""},
    ):
        result = classify_ack_orders_with_final_balance(
            provider=_CloseProvider(), trade_date="2026-10-13",
            orders=[{**_mck(), **change}],
        )
        assert result["pending_order_count"] > 0
        assert result["orders"][0]["final_status"] == "ack_unresolved_error"


def test_balance_without_sold_quantity_never_claims_fill():
    result = classify_ack_orders_with_final_balance(
        provider=_CloseProvider(qty=2), trade_date="2026-10-13",
        orders=[_mck()],
    )
    assert result["pending_order_count"] > 0
    assert result["orders"][0]["final_status"] == "ack_unresolved_error"
