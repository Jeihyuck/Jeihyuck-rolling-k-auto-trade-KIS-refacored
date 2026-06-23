import logging

import pandas as pd

from trader import pb1_runner


def test_close_final30_empty_bypass_and_kis_holding_sell_intent(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    monkeypatch.setenv("PB1_SESSION_KIND", "close")
    monkeypatch.setenv("PB1_ENTRY_ENABLED", "0")
    monkeypatch.setenv("PB1_CLOSE_LIQUIDATION_ENABLED", "1")

    df = pb1_runner._guard_empty_final30_for_engine_boot(
        pd.DataFrame(),
        phase_name="exit",
        session_kind="close",
    )
    assert df.empty

    orders = pb1_runner.build_close_liquidation_orders_from_kis_holdings([
        {"code": "000660", "qty": 1},
    ])

    assert len(orders) == 1
    assert orders[0]["code"] == "000660"
    assert orders[0]["side"] == "SELL"
    assert orders[0]["reason"] == "KR_CLOSE_LIQUIDATION_KIS_HOLDING"
    logs = caplog.text
    assert "TRADE_FINAL30_EMPTY_AFTER_ALL_FALLBACKS" not in logs
    assert "[TRADE][ENGINE_BOOT][FINAL30_BYPASS_FOR_CLOSE_EXIT]" in logs
    assert "[KR_CLOSE][SELL][INTENT] code=000660 qty=1 reason=KR_CLOSE_LIQUIDATION_KIS_HOLDING" in logs
    assert "EXIT_SHORTCIRCUIT" not in logs
