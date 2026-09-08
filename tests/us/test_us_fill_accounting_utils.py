from trader.us.db.fill_accounting_utils import qty_from_accounting_rows


def test_qty_from_accounting_rows_prefers_execution_qty():
    rows = [
        {"qty": 2, "meta": {"fill_evidence_type": "KIS_EXECUTION_ACTUAL"}},
        {"qty": 5, "meta": {"fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL", "cumulative_filled_qty": 5}},
    ]
    assert qty_from_accounting_rows(rows) == (2, 0, 2)


def test_qty_from_accounting_rows_tracks_synthetic_and_cumulative():
    rows = [
        {"qty": 3, "meta": {"fill_evidence_type": "LEGACY_SYNTHETIC", "cumulative_filled_qty": 3}},
        {"qty": 4, "meta": {"fill_evidence_type": "KIS_ORDER_DETAIL_ACTUAL", "cumulative_filled_qty": 4}},
    ]
    assert qty_from_accounting_rows(rows) == (4, 0, 4)
