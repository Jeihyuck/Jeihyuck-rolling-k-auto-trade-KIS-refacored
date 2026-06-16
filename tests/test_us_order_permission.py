from trader.us.execution.order_permissions import resolve_us_order_permissions

def test_am_afternoon_same_env_same_permission():
    env={'DRY_RUN':'0','DISABLE_LIVE_TRADING':'0','DISABLE_REAL_TRADING':'0','LIVE_TRADING_ENABLED':'1','US_LIVE_TRADING_ENABLED':'1','US_ORDER_ARMED':'1'}
    assert resolve_us_order_permissions('am','practice','TRADE',env)==resolve_us_order_permissions('afternoon','practice','TRADE',env)
    assert resolve_us_order_permissions('am','practice','TRADE',env).allowed
    assert 'live_trading_flag_enabled' not in resolve_us_order_permissions('am','practice','TRADE',env).reasons

def test_unarmed_reason():
    env={'DRY_RUN':'0','DISABLE_LIVE_TRADING':'0','LIVE_TRADING_ENABLED':'1','US_LIVE_TRADING_ENABLED':'1','US_ORDER_ARMED':'0'}
    p=resolve_us_order_permissions('am','practice','TRADE',env)
    assert not p.allowed and 'us_order_not_armed' in p.reasons
