from __future__ import annotations

import sys
import types
from datetime import datetime
from zoneinfo import ZoneInfo

from trader.kr.runner import trade_session_runner as runner


def test_close_session_forces_close_env_and_does_not_skip(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setenv("MARKET", "KR")
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KRX")
    monkeypatch.setattr(runner, "resolve_kr_trade_date", lambda _now: datetime(2026, 6, 17, tzinfo=ZoneInfo("Asia/Seoul")).date())
    monkeypatch.setattr(runner, "resolve_kr_expected_as_of", lambda _td: datetime(2026, 6, 16, tzinfo=ZoneInfo("Asia/Seoul")).date())
    monkeypatch.setattr(runner, "_assert_balance_available", lambda session: None)
    calls = {"n": 0}
    mod = types.SimpleNamespace(main=lambda: calls.__setitem__("n", calls["n"] + 1) or 0)
    monkeypatch.setitem(sys.modules, "trader.pb1_runner", mod)

    result = runner._run_pb1_session("close", "practice")

    assert result["status"] == "OK"
    assert calls["n"] == 1
    assert runner.os.environ["FORCE_PB1_PHASE"] == "close"
    assert runner.os.environ["FORCE_MARKET_WINDOW"] == "close"
    assert runner.os.environ["PB1_EXIT_ENABLED"] == "1"
    assert runner.os.environ["PB1_CLOSE_ENABLED"] == "1"
    assert runner.os.environ["PB1_CLOSE_LIQUIDATION_ENABLED"] == "1"
    assert runner.os.environ["KR_CLOSE_SESSION"] == "1"
    assert result["reason"] != "SKIP_PHASE_WINDOW"
