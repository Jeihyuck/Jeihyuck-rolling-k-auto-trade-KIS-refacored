from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from trader.kr.runner import trade_session_runner as runner


def test_balance_precheck_writes_snapshot(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setenv("KR_TRADE_DATE", "2026-06-17")
    snapshot = {"output1": [{"pdno": "005930", "hldg_qty": "2"}], "output2": {"dnca_tot_amt": "12345"}}

    class DummyKis:
        def get_balance_cached(self):
            return snapshot

    monkeypatch.setattr(runner, "KisAPI", lambda: DummyKis())
    assert runner._assert_balance_available("am") is None
    data = json.loads((tmp_path / "runtime/kr/session/2026-06-17/am/balance_precheck.json").read_text())
    assert data["state"] == "OK"
    assert data["raw_snapshot_available"] is True
    assert data["raw_snapshot"] == snapshot
    assert data["cash"] == 12345
    assert data["holdings_count"] == 1


def test_pb1_precheck_ok_without_snapshot_becomes_unknown(monkeypatch, tmp_path, caplog):
    import trader.pb1_runner as pb1_runner

    path = tmp_path / "balance_precheck.json"
    path.write_text(json.dumps({"state": "OK", "source": "KIS", "raw_snapshot_available": False, "entry_allowed": True, "exit_allowed": True, "close_allowed": True}), encoding="utf-8")
    monkeypatch.setenv("KR_BALANCE_PRECHECK_PATH", str(path))
    monkeypatch.setenv("MARKET", "KR")
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KRX")
    # Exercise the same branch through a tiny helper exposed for tests.
    state, snapshot, source = pb1_runner.resolve_kr_balance_precheck_for_test(path)
    assert state == pb1_runner.BALANCE_STATE_UNKNOWN
    assert snapshot is None
    assert source == "KIS"


def test_diagnostics_manifest_reads_balance_warning(monkeypatch, tmp_path):
    from trader.kr import diagnostics, artifacts

    monkeypatch.setattr(diagnostics, "ROOT", tmp_path)
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    trade_date = datetime(2026, 6, 17, tzinfo=ZoneInfo("Asia/Seoul")).date()
    expected_as_of = datetime(2026, 6, 16, tzinfo=ZoneInfo("Asia/Seoul")).date()
    balance_dir = tmp_path / "runtime/kr/session/2026-06-17/am"
    balance_dir.mkdir(parents=True)
    (balance_dir / "balance_precheck.json").write_text(
        json.dumps({"state": "OK", "source": "KIS", "raw_snapshot_available": False, "cash": 0, "holdings_count": 0, "positions_summary": {}}),
        encoding="utf-8",
    )

    path = diagnostics.write_kr_diagnostics_manifest(
        trade_date=trade_date,
        expected_as_of=expected_as_of,
        session="am",
        result={"status": "OK"},
        env="practice",
    )
    manifest = json.loads(path.read_text())
    assert manifest["balance"]["precheck_state"] == "UNKNOWN_WITHOUT_SNAPSHOT"
    assert manifest["balance"]["warning"] == "OK state without snapshot was downgraded"
