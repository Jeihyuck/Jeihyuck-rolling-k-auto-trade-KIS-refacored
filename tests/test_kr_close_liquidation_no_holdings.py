import logging

from trader import pb1_runner


def test_close_liquidation_no_holdings_skips(caplog):
    caplog.set_level(logging.INFO)
    orders = pb1_runner.build_close_liquidation_orders_from_kis_holdings([])
    assert orders == []
    assert "[KR_CLOSE][LIQUIDATION][SKIP] reason=NO_KIS_HOLDINGS" in caplog.text
