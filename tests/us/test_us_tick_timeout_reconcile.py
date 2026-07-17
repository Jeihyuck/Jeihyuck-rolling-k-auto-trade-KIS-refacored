from trader.us.execution.order_journal import replay_order_journal

def test_timeout_replay_marks_submit_without_ack_unresolved(tmp_path, monkeypatch):
    monkeypatch.setenv('US_ORDER_JOURNAL_DIR',str(tmp_path))
    from trader.us.execution.order_journal import append_order_event
    i={'trade_date':'2026-07-16','client_order_key':'k','symbol':'AMD','side':'SELL','qty':1,'tick_id':'t'}
    append_order_event('BROKER_SUBMIT_STARTED',i)
    r=replay_order_journal('2026-07-16',tick_id='t')
    assert r['status']=='UNRESOLVED' and r['unresolved_symbol_sides']==[['AMD','SELL']]
