from trader.us.execution.reconcile import confirm_order_by_balance_delta

def test_absent_symbol_without_pre_qty_is_unresolved():
    assert confirm_order_by_balance_delta('SELL',5,None,0)['status']=='RECONCILE_NEEDS_RECHECK'
