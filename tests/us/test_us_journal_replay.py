from trader.us.execution.order_journal import append_order_event,replay_order_journal

def test_ack_replay_restores_db_once(tmp_path,monkeypatch):
    monkeypatch.setenv('US_ORDER_JOURNAL_DIR',str(tmp_path)); i={'trade_date':'2026-07-16','client_order_key':'k','symbol':'AMD','exchange':'NASDAQ','side':'SELL','qty':2}
    append_order_event('BROKER_SUBMIT_STARTED',i); append_order_event('BROKER_ACK_RECEIVED',i,broker_order_no='O1')
    monkeypatch.setattr('trader.us.db.repos.save_order_ack',lambda *a,**k: True)
    r=replay_order_journal('2026-07-16'); assert r['restored_ack_count']==1 and r['unresolved_count']==1 and r['status']=='UNRESOLVED'
