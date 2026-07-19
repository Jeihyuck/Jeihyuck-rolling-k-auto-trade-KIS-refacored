from pathlib import Path

SRC=Path('scripts/repair_us_trade_integrity.py').read_text()


def test_repair_uses_nested_position_lifecycle_reconcile():
    assert 'reconcile_us_position_lifecycles' in SRC
    assert "jsonb_build_object('lifecycle'" in SRC
    assert 'lifecycle_status' not in SRC
