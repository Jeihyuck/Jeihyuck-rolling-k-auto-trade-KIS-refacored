from trader.us.execution.order_journal import replay_order_journal

def test_timeout_replay_marks_submit_without_ack_unresolved(tmp_path, monkeypatch):
    monkeypatch.setenv('US_ORDER_JOURNAL_DIR',str(tmp_path))
    from trader.us.execution.order_journal import append_order_event
    i={'trade_date':'2026-07-16','client_order_key':'k','symbol':'AMD','side':'SELL','qty':1,'tick_id':'t'}
    append_order_event('BROKER_SUBMIT_STARTED',i)
    r=replay_order_journal('2026-07-16',tick_id='t')
    assert r['status']=='UNRESOLVED' and r['unresolved_symbol_sides']==[['AMD','SELL']]

class Provider:
    def __init__(self,fill=None,orders=None): self.fill=fill; self.orders=orders or []; self.calls={'fill':0,'balance':0,'orders':0}; self._offline=True
    def get_balance(self,**kw): self.calls['balance']+=1; return {'positions':[],'balance_parse_status':'OK'}
    def get_today_orders(self,**kw): self.calls['orders']+=1; return self.orders
    def get_fills_by_order_no(self,**kw): self.calls['fill']+=1; return self.fill

def journal(tmp_path,monkeypatch,ack=True):
    monkeypatch.setenv('US_ORDER_JOURNAL_DIR',str(tmp_path)); from trader.us.execution.order_journal import append_order_event
    i={'trade_date':'2026-07-16','client_order_key':'k','symbol':'AMD','exchange':'NASDAQ','side':'SELL','qty':10,'tick_id':'t'}
    append_order_event('BROKER_SUBMIT_STARTED',i)
    if ack: append_order_event('BROKER_ACK_RECEIVED',i,broker_order_no='O1')

def test_timeout_replay_queries_kis_provider(tmp_path,monkeypatch):
    journal(tmp_path,monkeypatch); p=Provider(orders=[{'order_no':'O1','symbol':'AMD','side':'SELL','status':'ACK'}]); replay_order_journal('2026-07-16',tick_id='t',provider=p); assert p.calls=={'fill':1,'balance':1,'orders':1}
def test_db_ack_restore_alone_is_not_reconciled(tmp_path,monkeypatch):
    journal(tmp_path,monkeypatch); r=replay_order_journal('2026-07-16',tick_id='t'); assert r['db_ack_restored_count']==1 and r['status']=='UNRESOLVED'
def test_timeout_replay_full_fill(tmp_path,monkeypatch):
    import trader.us.db.repos as repos; repos.reset_memory_stores(); journal(tmp_path,monkeypatch); r=replay_order_journal('2026-07-16',tick_id='t',provider=Provider({'filled_qty':10,'avg_price':10,'symbol':'AMD','side':'SELL'})); assert r['broker_full_fill_count']==1
def test_timeout_replay_partial_fill(tmp_path,monkeypatch):
    import trader.us.db.repos as repos; repos.reset_memory_stores(); journal(tmp_path,monkeypatch); r=replay_order_journal('2026-07-16',tick_id='t',provider=Provider({'filled_qty':3,'avg_price':10,'symbol':'AMD','side':'SELL'})); assert r['broker_partial_fill_count']==1 and repos._MEM_ORDERS[0]['status']=='PARTIALLY_FILLED'
def test_timeout_replay_unfilled_ack(tmp_path,monkeypatch):
    journal(tmp_path,monkeypatch); p=Provider(orders=[{'order_no':'O1','symbol':'AMD','side':'SELL','status':'ACK'}]); r=replay_order_journal('2026-07-16',tick_id='t',provider=p); assert r['broker_unfilled_ack_count']==1 and r['status']=='OK'
def test_timeout_replay_submit_without_ack(tmp_path,monkeypatch):
    journal(tmp_path,monkeypatch,False); r=replay_order_journal('2026-07-16',tick_id='t',provider=Provider()); assert r['broker_unknown_count']==1
def test_timeout_replay_identity_mismatch(tmp_path,monkeypatch):
    journal(tmp_path,monkeypatch); r=replay_order_journal('2026-07-16',tick_id='t',provider=Provider({'filled_qty':1,'symbol':'AMZN','side':'SELL'})); assert r['identity_mismatch_count']==1
def test_timeout_replay_blocks_duplicate_symbol_side(tmp_path,monkeypatch):
    journal(tmp_path,monkeypatch,False); r=replay_order_journal('2026-07-16',tick_id='t',provider=Provider()); assert ['AMD','SELL'] in r['unresolved_symbol_sides']
