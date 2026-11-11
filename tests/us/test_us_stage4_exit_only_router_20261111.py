"""Stage4 EXIT_ONLY must fence every BUY at the canonical router boundary.

No DB writes, no native KIS, no real broker credentials, no network.
"""
import pytest

from trader.us.execution.order_router import route_order


@pytest.mark.parametrize("intent", [
    {"symbol": "AMGN", "side": "BUY", "qty": 3,
     "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD"},
    {"symbol": "TQQQ", "side": "BUY", "qty": 1,
     "strategy_owner": "TQQQ_INFINITE", "sleeve_id": "TQQQ_INFINITE"},
])
def test_exit_only_blocks_all_owner_buys_before_persistence_or_broker(
    monkeypatch, intent,
):
    monkeypatch.setenv("US_STAGE4_EXIT_ONLY_ACTIVE", "1")
    from trader.us.db import repos

    def never(*args, **kwargs):
        pytest.fail("EXIT_ONLY_BUY_MUST_NOT_REACH_DB_OR_KIS")

    monkeypatch.setattr(repos, "save_order_intent", never)

    class Broker:
        place_us_buy_order = staticmethod(never)
        place_us_sell_order = staticmethod(never)

    response = route_order({**intent, "trade_date": "2026-11-11",
                            "client_order_key": "test-stage4-exit-only"},
                           kis_client=Broker())
    assert response["status"] == "ORDER_DISABLED_STAGE4_EXIT_ONLY"
    assert response["broker_submit"] is False
    assert response["retry_order"] is False


def test_exit_only_does_not_replace_sell_validations(monkeypatch):
    """For SELL the real canonical identity check must still execute."""
    monkeypatch.setenv("US_STAGE4_EXIT_ONLY_ACTIVE", "1")
    response = route_order(
        {"symbol": "", "side": "SELL", "qty": 1,
         "trade_date": "2026-11-11", "client_order_key": "bad-sell-symbol"},
    )
    # The normal SELL identity validator is still called. The exit-only BUY
    # fence must neither authorize nor suppress a normal SELL evaluation.
    assert response["status"] == "INVALID_ORDER_IDENTITY"
