from trader.db.engine import _connect_args_for_db_url

def test_db_timeout_options(monkeypatch):
    args=_connect_args_for_db_url('postgresql+psycopg://u:p@h/db')
    opts=args['options']
    assert 'lock_timeout=5000' in opts
    assert 'statement_timeout=15000' in opts
    assert 'idle_in_transaction_session_timeout=15000' in opts
