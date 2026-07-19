from pathlib import Path

def test_migration_quarantines_before_unique_index():
    s=Path('migrations/0045_us_order_identity_integrity.sql').read_text()
    assert s.index('DUPLICATE_BROKER_ORDER_NUMBER') < s.index('uq_us_orders_trade_date_order_no_nonblank')
    assert 'DUPLICATE_CLIENT_KEY_IDENTITY' in s
