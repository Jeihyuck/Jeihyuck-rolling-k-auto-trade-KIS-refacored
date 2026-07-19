from trader.us.execution.reconcile import validate_reconcile_identity

def test_mismatch_and_ambiguity_are_rejected():
    row={'symbol':'AMZN','side':'SELL'}
    assert validate_reconcile_identity(trade_date='2026-07-16',order_no='1',requested_symbol='GOOGL',requested_side='SELL',matches=[row])['status']=='RECONCILE_SYMBOL_MISMATCH'
    assert validate_reconcile_identity(trade_date='2026-07-16',order_no='1',requested_symbol='AMZN',requested_side='SELL',matches=[row,row])['status']=='RECONCILE_ORDER_AMBIGUOUS'
