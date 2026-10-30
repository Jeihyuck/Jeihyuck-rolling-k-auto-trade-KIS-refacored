"""KR PR #267 lesson: never publish NaN/Infinity as US SELL profit."""
import math
import pytest
from trader.us.runner.daily_report_runner import (
    _positive_float,
    _safe_number,
    _sell_audit_pnl,
    _build_trade_reason_pnl_summary,
)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"),
                                    "NaN", "Infinity", "invalid"])
def test_nonfinite_inputs_are_not_valid_money(value):
    assert _positive_float(value) is None
    assert _safe_number(value) is None


def test_valid_sell_price_and_realized_gross_pnl_unchanged():
    audit = _sell_audit_pnl(
        {"fill_price": 110, "qty_filled": 2},
        {"entry_price": 100}, {}, "FILLED"
    )
    assert audit["fill_price"] == 110
    assert audit["gross_realized_pnl"] == 20
    assert not audit["missing_fill_price"]


def test_nonfinite_sell_price_leaves_pnl_unknown_not_infinite():
    audit = _sell_audit_pnl(
        {"fill_price": float("inf"), "qty_filled": 2},
        {"entry_price": 100, "gross_realized_pnl": float("nan")},
        {"fill_price": "NaN"}, "FILLED",
    )
    assert audit["fill_price"] is None
    assert audit["gross_realized_pnl"] is None
    assert audit["missing_fill_price"] is True
    assert audit["error"] == "ERROR_MISSING_SELL_FILL_PRICE"


def test_invalid_persisted_gross_is_recomputed_only_with_finite_proof():
    audit = _sell_audit_pnl(
        {"fill_price": 110, "qty_filled": 3},
        {"entry_price": 105, "gross_realized_pnl": float("inf")},
        {}, "FILLED",
    )
    assert audit["gross_realized_pnl"] == 15


def test_unknown_filled_sell_pnl_explicitly_marked_not_verified_zero():
    result = _build_trade_reason_pnl_summary(
        [{"symbol": "SYY", "side": "SELL", "lifecycle": {"filled": True},
          "canonical_order_no": "123", "filled_qty": 2,
          "gross_realized_pnl": float("nan")}],
        [], [],
    )
    assert result["realized_pnl_status"] == "PARTIAL_UNVERIFIED"
    assert result["realized_trade_count_with_pnl"] == 0


def test_no_sell_day_zero_realized_profit_is_verified():
    result = _build_trade_reason_pnl_summary([], [], [])
    assert result["realized_pnl_status"] == "VERIFIED"
    assert result["realized_pnl_usd"] == 0
