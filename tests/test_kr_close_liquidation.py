import logging

from trader.pb1_runner import run_close_liquidation_from_kis_holdings


def test_close_liquidation_uses_kis_holdings_even_without_db_positions(caplog):
    caplog.set_level(logging.INFO)
    calls = []

    def submit_sell_order(**kwargs):
        calls.append(kwargs)
        return {"rt_cd": "0", "msg_cd": "OK", "msg1": "accepted"}

    results = run_close_liquidation_from_kis_holdings(
        kis_client=object(),
        holdings=[{"code": "000660", "qty": 1}],
        submit_sell_order=submit_sell_order,
    )

    assert len(results) == 1
    assert len(calls) == 1
    assert calls[0]["code"] == "000660"
    assert calls[0]["qty"] == 1
    assert "[KR_CLOSE][SELL][INTENT] code=000660 qty=1 reason=KR_CLOSE_LIQUIDATION_KIS_HOLDING" in caplog.text
    assert "[ORDER][API_CALL][START] side=SELL code=000660" in caplog.text
    assert "EXIT_SHORTCIRCUIT" not in caplog.text
