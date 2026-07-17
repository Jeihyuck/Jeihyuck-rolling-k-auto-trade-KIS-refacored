from trader.us.execution.reconcile import confirm_order_by_balance_delta,validate_reconcile_identity
from trader.us.runner.daily_report_runner import _position_market_value_usd

def test_incident_invariants():
    assert confirm_order_by_balance_delta('SELL',10,30,21)['status']=='BALANCE_CONFIRMED_PARTIAL'
    assert validate_reconcile_identity(trade_date='2026-07-16',order_no='x',requested_symbol='GOOGL',requested_side='SELL',matches=[{'symbol':'AMZN','side':'SELL'}])['status']=='RECONCILE_SYMBOL_MISMATCH'
    positions=[{'qty':1,'current_px':100+i} for i in range(21)]
    assert len(positions)==21 and sum(map(_position_market_value_usd,positions))>0
