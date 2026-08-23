from trader.us.execution.reconcile import confirm_order_by_balance_delta

def test_bac_exact_sell_balance_delta_confirms():
    result = confirm_order_by_balance_delta("SELL", 3, 7, 4)
    assert result["status"] == "BALANCE_CONFIRMED_SELL"
    assert result["pending"] is False

def test_inexact_sell_balance_delta_stays_pending():
    result = confirm_order_by_balance_delta("SELL", 3, 7, 5)
    assert result["status"] != "BALANCE_CONFIRMED_SELL"
    assert result["pending"] is True
