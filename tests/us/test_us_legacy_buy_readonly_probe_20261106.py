"""US legacy original BUY inquiry is KIS practice GET-only and fail-closed."""
import pytest
from scripts.us_original_order_readonly_probe import classify_original_order

DAY = "2026-10-30"
ORDER_NO = "0000036019"


def original_buy(*, symbol="TMO", filled="5", remaining="0", qty="5", side="02"):
    return {
        "odno": ORDER_NO, "pdno": symbol, "sll_buy_dvsn_cd": side,
        "ft_ord_qty": qty, "ft_ccld_qty": filled, "nccs_qty": remaining,
        "ord_dt": "20261030", "ord_tmd": "223010",
        "dmst_ord_dt": "20261030",
    }


def classify(rows, *, side="BUY", qty=5):
    return classify_original_order(rows, order_no=ORDER_NO, symbol="TMO",
                                   expected_qty=qty, trade_date=DAY, side=side)


def test_authenticated_original_buy_full_fill_candidate():
    r=classify([original_buy()])
    assert r["classification"] == "PROVEN_FILLED"
    assert r["side"] == "BUY"
    assert r["orders_submitted"] == r["cancels_submitted"] == 0


def test_original_buy_wrong_side_cannot_use_existing_sell_order():
    assert classify([original_buy(side="01")])["classification"] == "UNKNOWN"
    assert classify([original_buy()], side="SELL")["classification"] == "UNKNOWN"


def test_original_buy_partial_fill_is_not_final_fill():
    assert classify([original_buy(filled="2", remaining="3")])["classification"] == "PROVEN_OPEN"


def test_original_buy_order_qty_and_trade_date_must_match():
    assert classify([original_buy(qty="4")])["classification"] == "UNKNOWN"
    wrong = original_buy()
    wrong["ord_dt"] = "20261029"
    assert classify([wrong])["classification"] == "UNKNOWN"


def test_original_buy_duplicate_order_is_ambiguous():
    assert classify([original_buy(), original_buy()])["classification"] == "UNKNOWN"


def test_original_buy_missing_broker_cumulative_proof_is_unknown():
    missing=original_buy()
    missing.pop("nccs_qty")
    assert classify([missing])["classification"] == "UNKNOWN"


def test_original_probe_rejects_invalid_side():
    with pytest.raises(ValueError, match="ORIGINAL_KIS_ORDER_SIDE_INVALID"):
        classify([original_buy()], side="HOLD")
