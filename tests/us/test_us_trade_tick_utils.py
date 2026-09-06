from trader.us.runner.trade_tick_utils import (
    aggregate_fill_notionals,
    fill_is_synthetic,
    fill_notional_usd,
    is_transient_watchlist_db_error,
    normalize_kis_endpoint_name,
    safe_float,
)


def test_trade_tick_utils_safe_float_and_endpoint():
    assert safe_float("3.5") == 3.5
    assert safe_float(None) == 0.0
    assert normalize_kis_endpoint_name("GET_inquire_balance") == "GET_inquire-balance"


def test_trade_tick_utils_fill_notional_and_synthetic():
    assert fill_is_synthetic({"meta": {"synthetic": True}})
    assert fill_notional_usd({"qty": 2, "fill_price": 5}) == 10.0
    assert aggregate_fill_notionals([
        {"side": "BUY", "qty": 2, "fill_price": 5},
        {"side": "SELL", "meta": {"synthetic_fill": True}},
    ]) == (10.0, 0.0)


def test_trade_tick_utils_transient_db_error():
    assert is_transient_watchlist_db_error(RuntimeError("statement timeout"))
