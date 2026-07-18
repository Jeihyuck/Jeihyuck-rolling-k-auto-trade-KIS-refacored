from pathlib import Path
SRC=Path('scripts/repair_us_trade_integrity.py').read_text()

def test_repair_selects_cumulative_snapshot_instead_of_summing_inquire_ccnl_rows():
    assert 'grouped_actual = {}' in SRC
    forbidden = 'grouped_actual[key]["qty"]' + ' += qty'
    assert forbidden not in SRC
    assert 'canonical_kis_order_cumulative_key' in SRC
    assert 'KIS_ORDER_CUMULATIVE_ACTUAL' in SRC
