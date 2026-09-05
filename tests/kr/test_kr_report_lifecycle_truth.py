from trader.execution_state import PENDING_SELL_STATES


def test_kr_report_no_sellable_is_not_fill_or_success():
    assert "NO_SELLABLE_QTY" not in PENDING_SELL_STATES
