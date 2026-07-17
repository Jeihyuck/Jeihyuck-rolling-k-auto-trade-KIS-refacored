from trader.us.execution.order_router import enrich_sell_exchange
from trader.us.execution.tick_context import TickExecutionContext

def test_sell_exchange_uses_context_without_balance_call():
    class K:
        def get_balance(self, **kw): raise AssertionError('must not fetch')
    c=TickExecutionContext('2026-07-16','am','r',1,'t',exchange_by_symbol={'AMD':'NASDAQ'})
    i={'symbol':'AMD','side':'SELL'}
    assert enrich_sell_exchange(i,K(),c)=='NASDAQ'
