import pytest

from trader.kr.realized_pnl_truth import verified_sell_fill_price, classify_sell_pnl_evidence


@pytest.mark.parametrize("bad", [None, "", "invalid", 0, -1, float("nan"), float("inf"), -float("inf")])
def test_unproven_price_never_becomes_realized_pnl(bad):
    assert verified_sell_fill_price(bad) is None
    assert classify_sell_pnl_evidence(confirmed_qty=1, accounted_qty=0, price=bad) == "REALIZED_PNL_UNRESOLVED"


def test_valid_price_requires_qty_attribution():
    assert verified_sell_fill_price("12345.50") == 12345.5
    assert classify_sell_pnl_evidence(confirmed_qty=2, accounted_qty=2, price=12345.5) == "CONFIRMED"
    assert classify_sell_pnl_evidence(confirmed_qty=2, accounted_qty=1, price=12345.5) == "PARTIAL_PRICE_PROOF"
    assert classify_sell_pnl_evidence(confirmed_qty=1, accounted_qty=2, price=12345.5) == "INVALID_QTY_PROVENANCE"


def test_no_nonfinite_or_boolean_price_or_accounting_inputs():
    from trader.kr.realized_pnl_truth import verified_sell_fill_price, verified_finite_amount
    for value in (True, False, float("nan"), float("inf"), float("-inf"), "garbage"):
        assert verified_sell_fill_price(value) is None
        assert verified_finite_amount(value) is None
    assert verified_finite_amount(-10.0) == -10.0
    assert verified_finite_amount(-10.0, nonnegative=True) is None
    assert verified_finite_amount(0, nonnegative=True) == 0.0
    assert verified_finite_amount(123.4) == 123.4
