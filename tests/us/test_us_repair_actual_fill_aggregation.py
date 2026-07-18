from pathlib import Path

SRC=Path('scripts/repair_us_trade_integrity.py').read_text()


def test_repair_groups_actual_fills_by_order_before_update():
    assert 'grouped_actual = defaultdict' in SRC
    assert 'grouped_actual[key]["qty"] += qty' in SRC
    assert 'weighted_avg' not in SRC or 'avg_price' in SRC


def test_repair_reconstructs_missing_order_or_quarantines_identity():
    assert 'repair_reconstructed' in SRC
    assert 'REPAIR_ORDER_IDENTITY_UNRESOLVED' in SRC
