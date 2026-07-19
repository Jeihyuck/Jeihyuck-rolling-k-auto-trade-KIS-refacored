from pathlib import Path

SQL=Path('migrations/0045_us_order_identity_integrity.sql').read_text()


def test_duplicate_client_key_quarantines_intents_fills_orders_same_batch():
    assert 'DUPLICATE_CLIENT_KEY_IDENTITY_RELATED_INTENT' in SQL
    assert 'DUPLICATE_CLIENT_KEY_IDENTITY_RELATED_FILL' in SQL
    assert "md5('client|'||" in SQL
    assert 'DELETE FROM us_order_intents i USING' in SQL
    assert 'DELETE FROM us_fills f USING' in SQL


def test_duplicate_client_key_dependency_cleanup_precedes_order_delete():
    assert SQL.index('DUPLICATE_CLIENT_KEY_IDENTITY_RELATED_INTENT') < SQL.index("DELETE FROM us_orders o USING")
