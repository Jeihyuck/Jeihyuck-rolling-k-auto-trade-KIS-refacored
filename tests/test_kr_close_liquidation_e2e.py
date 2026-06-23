import logging

import pandas as pd

from trader import pb1_runner


class FakeKis:
    def __init__(self, holdings):
        self.holdings = holdings
        self.sell_calls = []

    def get_balance(self):
        return {"output1": self.holdings}

    def sell_stock_market(self, code, qty):
        self.sell_calls.append({"code": code, "qty": qty, "reason": "KR_CLOSE_LIQUIDATION_KIS_HOLDING"})
        return {"rt_cd": "0", "msg_cd": "OK", "msg1": "accepted", "output": {"ODNO": "1"}}


def test_close_path_empty_final30_allows_kis_sell_submit(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    monkeypatch.setenv("PB1_SESSION_KIND", "close")
    monkeypatch.setenv("FORCE_PB1_PHASE", "exit")
    monkeypatch.setenv("PB1_ENTRY_ENABLED", "0")
    monkeypatch.setenv("PB1_CLOSE_LIQUIDATION_ENABLED", "1")

    pb1_runner._validate_trade_locked_final30_or_raise(
        final30_df=pd.DataFrame(),
        source_name="none",
        allow_empty_for_close_exit=True,
    )
    guarded = pb1_runner._guard_empty_final30_for_engine_boot(pd.DataFrame(), phase_name="exit", session_kind="close")
    assert guarded.empty

    fake_kis = FakeKis([{"code": "000660", "qty": 1}])
    results = pb1_runner.run_close_liquidation_from_kis_holdings(kis_client=fake_kis, env="practice")

    assert fake_kis.sell_calls == [{"code": "000660", "qty": 1, "reason": "KR_CLOSE_LIQUIDATION_KIS_HOLDING"}]
    assert len(results) == 1
    logs = caplog.text
    assert "TRADE_FINAL30_EMPTY_AFTER_ALL_FALLBACKS" not in logs
    assert "DB_EXACT_FINAL30_ZERO" not in logs
    assert "EXIT_SHORTCIRCUIT" not in logs
    assert "[ORDER][API_CALL][START] side=SELL code=000660" in logs
    assert "[KIS][ORDER][RESPONSE] side=SELL code=000660 rt_cd=0" in logs
    assert "[TRADE][ORDER][SELL] code=000660 result=ACCEPTED" in logs


def test_close_path_no_holdings_skips_normally(caplog):
    caplog.set_level(logging.INFO)
    fake_kis = FakeKis([])
    results = pb1_runner.run_close_liquidation_from_kis_holdings(kis_client=fake_kis, env="practice")
    assert results == []
    assert fake_kis.sell_calls == []
    assert "[KR_CLOSE][LIQUIDATION][SKIP] reason=NO_KIS_HOLDINGS" in caplog.text
