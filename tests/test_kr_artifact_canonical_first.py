from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from trader.kr import artifacts


def _rows(as_of: date) -> list[dict]:
    return [{"code": f"{i:06d}", "as_of": as_of.isoformat(), "rank_final30": i + 1} for i in range(30)]


def test_canonical_ok_legacy_exists_is_not_fatal(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    monkeypatch.setenv("KR_BLOCK_LEGACY_ARTIFACT", "1")
    monkeypatch.setenv("KR_QUARANTINE_STALE_ARTIFACT", "0")
    trade_date = date(2026, 6, 18)
    expected = date(2026, 6, 17)
    artifacts.publish_kr_prep_artifacts_atomic(
        trade_date=trade_date,
        expected_as_of=expected,
        actual_as_of=expected,
        env="practice",
        final30_rows=_rows(expected),
        db_exact_rows=30,
        metadata={},
    )
    legacy = tmp_path / "signals/final30.json"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(json.dumps({"as_of": expected.isoformat(), "rows": _rows(expected)}), encoding="utf-8")

    result = artifacts.validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected, env="practice", strict=True, allow_legacy_fallback=False)

    assert result.ok is True
    assert result.source == "canonical"
    assert result.reason == "CANONICAL_OK"
    assert result.legacy_blocked is True
    assert result.fatal is False


def test_canonical_missing_legacy_exists_strict_fails_canonical_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    monkeypatch.setenv("KR_BLOCK_LEGACY_ARTIFACT", "1")
    monkeypatch.setenv("KR_QUARANTINE_STALE_ARTIFACT", "0")
    trade_date = date(2026, 6, 18)
    expected = date(2026, 6, 17)
    legacy = tmp_path / "signals/final30.json"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(json.dumps({"as_of": expected.isoformat(), "rows": _rows(expected)}), encoding="utf-8")

    result = artifacts.validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected, env="practice", strict=True, allow_legacy_fallback=False)

    assert result.ok is False
    assert result.fatal is True
    assert result.reason == "CANONICAL_MISSING"


def test_am_precheck_logs_canonical_ok_with_legacy(tmp_path, monkeypatch, caplog):
    from trader.kr.runner import trade_session_runner as runner
    trade_date = date(2026, 6, 18)
    expected = date(2026, 6, 17)
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setenv("KR_TRADE_DATE", trade_date.isoformat())
    monkeypatch.setenv("KR_EXPECTED_AS_OF", expected.isoformat())
    monkeypatch.setenv("KR_QUARANTINE_STALE_ARTIFACT", "0")
    artifacts.publish_kr_prep_artifacts_atomic(trade_date=trade_date, expected_as_of=expected, actual_as_of=expected, env="practice", final30_rows=_rows(expected), db_exact_rows=30, metadata={})
    legacy = tmp_path / "signals/final30.json"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(json.dumps({"as_of": expected.isoformat(), "rows": _rows(expected)}), encoding="utf-8")
    ctx = runner._session_context("am", "practice")

    with caplog.at_level("INFO"):
        result = runner._guard_trade_session("am", ctx)

    assert result is None
    assert "[KR_SESSION][PRECHECK_OK] session=am source=canonical rows=30" in caplog.text
    assert "LEGACY_SOURCE_NOT_ALLOWED" not in caplog.text


def test_close_balance_timeout_summary_warn(monkeypatch, tmp_path, caplog):
    import sys, types
    from trader.kis_wrapper import KisBalanceUnavailable
    from trader.kr.runner import trade_session_runner as runner
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setenv("KR_TRADE_DATE", "2026-06-18")
    monkeypatch.setenv("KR_EXPECTED_AS_OF", "2026-06-17")

    class DummyKis:
        def get_balance_cached(self):
            raise KisBalanceUnavailable("timeout")

    monkeypatch.setattr(runner, "KisAPI", lambda: DummyKis())
    monkeypatch.setattr(runner, "resolve_kr_balance_fail_soft", lambda exc, env="practice": {"status": "WARN", "reason": "BALANCE_TIMEOUT_FAIL_SOFT", "entry_allowed": False, "exit_allowed": True, "order_allowed": 0})
    fake_pb1 = types.SimpleNamespace(main=lambda: 0)
    monkeypatch.setitem(sys.modules, "trader.pb1_runner", fake_pb1)
    import trader
    monkeypatch.setattr(trader, "pb1_runner", fake_pb1, raising=False)

    with caplog.at_level("INFO"):
        result = runner._run_pb1_session("close", "practice")

    assert result["status"] == "WARN"
    assert result["reason"] == "CLOSE_BALANCE_UNCONFIRMED"
    assert result["balance_state"] == "TIMEOUT"
    assert "status=WARN reason=CLOSE_BALANCE_UNCONFIRMED" in caplog.text
    assert "status=OK reason=PB1_SESSION_DONE" not in caplog.text


def test_am_duplicate_lock_skips_second_process(tmp_path):
    import fcntl
    import subprocess
    from pathlib import Path

    lock_dir = Path("runtime/locks")
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / "kr-am.lock"
    with lock_path.open("w") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        proc = subprocess.run(["bash", "scripts/wsl/run-kr-am.sh"], text=True, capture_output=True, timeout=10)
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    assert proc.returncode == 0
    assert "[KR_AM][SKIP] reason=LOCK_HELD" in proc.stdout


def test_publish_validation_does_not_fallback_to_legacy_when_canonical_corrupt(tmp_path, monkeypatch):
    import os
    import pytest

    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    monkeypatch.setenv("KR_QUARANTINE_STALE_ARTIFACT", "0")
    trade_date = date(2026, 6, 18)
    expected = date(2026, 6, 17)
    legacy = tmp_path / "signals/final30.json"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(json.dumps({"as_of": expected.isoformat(), "rows": _rows(expected)}), encoding="utf-8")
    real_replace = os.replace

    def corrupt_after_replace(src, dst):
        real_replace(src, dst)
        if str(dst).endswith("latest_prep_contract.json"):
            runtime_final = tmp_path / "runtime/kr/watchlist" / trade_date.isoformat() / "final30_scored.json"
            payload = json.loads(runtime_final.read_text(encoding="utf-8"))
            payload["rows"] = payload["rows"][:29]
            runtime_final.write_text(json.dumps(payload), encoding="utf-8")

    monkeypatch.setattr(artifacts.os, "replace", corrupt_after_replace)

    with pytest.raises(RuntimeError):
        artifacts.publish_kr_prep_artifacts_atomic(
            trade_date=trade_date,
            expected_as_of=expected,
            actual_as_of=expected,
            env="practice",
            final30_rows=_rows(expected),
            db_exact_rows=30,
            metadata={},
        )


def test_kr_signal_legacy_paths_ignored_when_canonical_valid(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    monkeypatch.setenv("KR_QUARANTINE_STALE_ARTIFACT", "0")
    trade_date = date(2026, 6, 18)
    expected = date(2026, 6, 17)
    artifacts.publish_kr_prep_artifacts_atomic(trade_date=trade_date, expected_as_of=expected, actual_as_of=expected, env="practice", final30_rows=_rows(expected), db_exact_rows=30, metadata={})
    legacy_final = tmp_path / "signals/kr/final30_scored.json"
    legacy_contract = tmp_path / "signals/kr/prep_contract.json"
    legacy_final.parent.mkdir(parents=True, exist_ok=True)
    legacy_final.write_text(json.dumps({"as_of": expected.isoformat(), "rows": _rows(expected)}), encoding="utf-8")
    legacy_contract.write_text(json.dumps({"trade_date": trade_date.isoformat(), "as_of": expected.isoformat(), "rows": 30}), encoding="utf-8")

    result = artifacts.validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected, env="practice", strict=True, allow_legacy_fallback=False)

    assert result.ok is True
    assert result.source == "canonical"
    assert result.legacy_blocked is True
    assert "signals/kr/final30_scored.json" in result.legacy_paths
    assert "signals/kr/prep_contract.json" in result.legacy_paths


def test_canonical_missing_kr_signal_legacy_only_strict_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    monkeypatch.setenv("KR_QUARANTINE_STALE_ARTIFACT", "0")
    trade_date = date(2026, 6, 18)
    expected = date(2026, 6, 17)
    legacy_final = tmp_path / "signals/kr/final30_scored.json"
    legacy_final.parent.mkdir(parents=True, exist_ok=True)
    legacy_final.write_text(json.dumps({"as_of": expected.isoformat(), "rows": _rows(expected)}), encoding="utf-8")

    result = artifacts.validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected, env="practice", strict=True, allow_legacy_fallback=False)

    assert result.ok is False
    assert result.fatal is True
    assert result.reason == "CANONICAL_MISSING"
    assert result.legacy_blocked is True
    assert result.legacy_paths == ["signals/kr/final30_scored.json"]
