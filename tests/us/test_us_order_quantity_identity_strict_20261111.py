"""US native order identity must never round or coerce broker share counts.

Pure offline regression: no KIS order, network, DB, trading epoch mutation.
"""
from decimal import Decimal

import pytest

from trader.us.execution.order_identity import (
    InvalidOrderIdentity,
    exact_order_share_quantity,
    normalize_and_validate_order_identity,
)


@pytest.mark.parametrize("raw", [True, False, 6.5, 6.0, -1, 0, None, "6.0", " 6", "-1", "", Decimal("6"), float("nan"), float("inf")])
def test_bad_share_quantity_fail_closed_before_any_order_row(raw):
    with pytest.raises(InvalidOrderIdentity, match="positive whole share"):
        exact_order_share_quantity(raw)
    with pytest.raises(InvalidOrderIdentity, match="positive whole share"):
        normalize_and_validate_order_identity({
            "symbol": "AMGN", "side": "BUY",
            "trade_date": "2026-11-11", "qty": raw,
            "client_order_key": "fixture-identity",
        })


@pytest.mark.parametrize("raw,expected", [(6, 6), ("6", 6), ("0006", 6), (1, 1)])
def test_exact_whole_share_inputs_remain_orderable(raw, expected):
    got = normalize_and_validate_order_identity({
        "symbol": "amgn", "side": "BUY",
        "trade_date": "2026-11-11", "qty": raw,
        "client_order_key": "fixture-identity",
    })
    assert got["qty"] == expected
    assert got["symbol"] == "AMGN"
    assert got["client_order_key"] == "fixture-identity"


def test_legacy_quantity_alias_is_checked_with_identical_policy():
    got = normalize_and_validate_order_identity({
        "symbol": "AMGN", "side": "SELL",
        "trade_date": "2026-11-11", "quantity": "3",
        "client_order_key": "fixture-sell",
    })
    assert got["qty"] == 3
    with pytest.raises(InvalidOrderIdentity):
        normalize_and_validate_order_identity({
            "symbol": "AMGN", "side": "SELL",
            "trade_date": "2026-11-11", "quantity": 3.5,
            "client_order_key": "fixture-sell",
        })



@pytest.mark.parametrize("side,raw", [
    ("BUY", 6.5), ("BUY", True), ("BUY", "6.0"),
    ("SELL", 2.5), ("SELL", False), ("SELL", None),
])
def test_actual_router_denies_invalid_qty_before_db_and_kis(monkeypatch, side, raw):
    from trader.us.db import repos
    from trader.us.execution.order_router import route_order

    def unexpected(*args, **kwargs):
        pytest.fail("INVALID_QUANTITY_MUST_NOT_REACH_DB_OR_KIS")

    monkeypatch.setattr(repos, "save_order_intent", unexpected)
    class NoBroker:
        place_us_buy_order = staticmethod(unexpected)
        place_us_sell_order = staticmethod(unexpected)

    got = route_order(
        {
            "symbol": "AMGN", "side": side, "qty": raw,
            "exchange": "NASDAQ", "trade_date": "2026-11-11",
            "client_order_key": "bad-share-proof",
        },
        kis_client=NoBroker(),
    )
    assert got["status"] == "INVALID_ORDER_IDENTITY"
    assert got["broker_submit"] is False
