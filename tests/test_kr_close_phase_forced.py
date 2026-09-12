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

    def fake_success_main():
        calls["n"] += 1
        result_path = runner.Path(runner.os.environ["PB1_SESSION_RESULT_PATH"])
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(
            runner.json.dumps({"status": "OK", "entry_status": "DONE", "sell_orders_ack": 0}),
            encoding="utf-8",
        )
        return 0

    mod = types.SimpleNamespace(main=fake_success_main)
    monkeypatch.setitem(sys.modules, "trader.pb1_runner", mod)

    result = runner._run_pb1_session("close", "practice")

    assert result["status"] == "OK"
    assert calls["n"] == 1
    assert runner.os.environ["FORCE_PB1_PHASE"] == "exit"
    assert runner.os.environ["FORCE_MARKET_WINDOW"] == "close"
    assert runner.os.environ["PB1_EXIT_ENABLED"] == "1"
    assert runner.os.environ["PB1_CLOSE_ENABLED"] == "1"
    assert runner.os.environ["PB1_CLOSE_LIQUIDATION_ENABLED"] == "0"
    assert runner.os.environ["KR_CLOSE_SESSION"] == "1"
    assert result["reason"] != "SKIP_PHASE_WINDOW"


def test_run_session_close_treats_skip_phase_window_as_fail(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setenv("MARKET", "KR")
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KRX")
    monkeypatch.setattr(runner, "resolve_kr_trade_date", lambda _now: datetime(2026, 6, 17, tzinfo=ZoneInfo("Asia/Seoul")).date())
    monkeypatch.setattr(runner, "resolve_kr_expected_as_of", lambda _td: datetime(2026, 6, 16, tzinfo=ZoneInfo("Asia/Seoul")).date())
    monkeypatch.setattr(runner, "_assert_balance_available", lambda session: None)

    def fake_main():
        runner.os.environ["PB1_LAST_RESULT_STATUS"] = "SKIP_PHASE_WINDOW"
        return 0

    monkeypatch.setitem(sys.modules, "trader.pb1_runner", types.SimpleNamespace(main=fake_main))
    result = runner._run_pb1_session("close", "practice")
    assert result["status"] == "FAIL"
    assert result["skip_phase_window"] is True


def test_diagnostics_manifest_reads_close_phase_marker(monkeypatch, tmp_path):
    import json
    from trader.kr import diagnostics, artifacts

    monkeypatch.setattr(diagnostics, "ROOT", tmp_path)
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    trade_date = datetime(2026, 6, 17, tzinfo=ZoneInfo("Asia/Seoul")).date()
    expected_as_of = datetime(2026, 6, 16, tzinfo=ZoneInfo("Asia/Seoul")).date()
    phase_dir = tmp_path / "runtime/kr/session/2026-06-17/close"
    phase_dir.mkdir(parents=True)
    (phase_dir / "phase.json").write_text(
        json.dumps({
            "phase": "close",
            "phase_executed": True,
            "force_phase": True,
            "skip_phase_window": False,
            "entry_enabled": False,
            "exit_enabled": True,
            "close_enabled": True,
            "close_liquidation_enabled": False,
        }),
        encoding="utf-8",
    )
    path = diagnostics.write_kr_diagnostics_manifest(
        trade_date=trade_date,
        expected_as_of=expected_as_of,
        session="close",
        result={"status": "OK", "phase_executed": True},
        env="practice",
    )
    manifest = json.loads(path.read_text())
    assert manifest["close"] == {
        "phase": "close",
        "phase_executed": True,
        "force_phase": True,
        "skip_phase_window": False,
        "entry_enabled": False,
        "exit_enabled": True,
        "close_enabled": True,
        "close_liquidation_enabled": False,
    }
