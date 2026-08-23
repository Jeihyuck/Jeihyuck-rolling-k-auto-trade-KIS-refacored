import pytest
from trader.us.market_state_overlay import build_defense_trim_intents
from trader.us.runner.daily_report_runner import _sell_audit_pnl

@pytest.mark.parametrize("symbol,avg,fill,expected", [
    ("MSFT", 482.50, 483.55, 1.05), ("AMZN", 273.455, 259.37, -14.085),
    ("SNOW", 330.45, 321.48, -8.97), ("NVDA", 225.78, 216.09, -9.69),
])
def test_defense_trim_preserves_cost_basis_and_reports_pnl(monkeypatch, symbol, avg, fill, expected):
    monkeypatch.setattr("trader.us.db.repos.has_same_day_exit", lambda *a, **k: False)
    intent = build_defense_trim_intents(
        [{"symbol": symbol, "qty": 4, "current_price": fill, "avg_price": avg,
          "theme_cluster": "AI_SEMICONDUCTOR", "position_lifecycle_id": "L1"}],
        {"market_state": "DEFENSE_RISK_OFF"}, trade_date="2026-08-21",
    )[0]
    assert intent["broker_avg_price"] == pytest.approx(avg)
    assert intent["meta"]["pre_order_holding_qty"] == 4
    result = _sell_audit_pnl({}, intent["meta"], {"fill_price": fill, "filled_qty": 1}, "FILLED")
    assert result["gross_realized_pnl"] == pytest.approx(expected)

def test_missing_sell_cost_basis_is_explicit_error():
    result = _sell_audit_pnl({}, {"reason": "SOFT_STOP"}, {"fill_price": 10, "filled_qty": 1}, "FILLED")
    assert result["gross_realized_pnl"] is None
    assert result["error"] == "ERROR_MISSING_SELL_COST_BASIS"
