from __future__ import annotations

import json
from datetime import datetime, timedelta

from trader.kr import artifacts
from trader.kr.runner.session_policy import KST
from trader.kr.calendar import resolve_kr_expected_as_of


def _rows(expected, offset=0):
    return [{"code": f"{i+offset:06d}", "as_of": expected.isoformat(), "rank_final30": i + 1} for i in range(30)]


def test_payload_mismatch_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    trade_date = datetime.now(KST).date()
    while trade_date.weekday() >= 5:
        trade_date += timedelta(days=1)
    expected = resolve_kr_expected_as_of(trade_date)
    artifacts.publish_kr_prep_artifacts_atomic(trade_date=trade_date, expected_as_of=expected, actual_as_of=expected, env="practice", final30_rows=_rows(expected), db_exact_rows=30, metadata={})
    runtime = tmp_path / "runtime/kr/watchlist" / trade_date.isoformat() / "final30_scored.json"
    payload = json.loads(runtime.read_text())
    payload["rows"] = _rows(expected, offset=100)
    runtime.write_text(json.dumps(payload), encoding="utf-8")
    res = artifacts.validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected, env="practice", allow_legacy=True)
    assert res.ok is False
    assert res.reason == "CODE_LIST_MISMATCH"


def test_valid_canonical_contracts_are_kept_and_legacy_quarantined(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    trade_date = datetime.now(KST).date()
    while trade_date.weekday() >= 5:
        trade_date += timedelta(days=1)
    expected = resolve_kr_expected_as_of(trade_date)
    artifacts.publish_kr_prep_artifacts_atomic(trade_date=trade_date, expected_as_of=expected, actual_as_of=expected, env="practice", final30_rows=_rows(expected), db_exact_rows=30, metadata={})
    legacy = tmp_path / "signals/final30.json"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(json.dumps({"rows": _rows(expected)}), encoding="utf-8")

    artifacts.quarantine_stale_kr_artifacts(trade_date=trade_date, expected_as_of=expected, env="practice")
    assert (tmp_path / "signals/kr/latest_final30_scored.json").exists()
    assert (tmp_path / "signals/kr/latest_prep_contract.json").exists()
    assert (tmp_path / "runtime/kr/watchlist" / trade_date.isoformat() / "final30_scored.json").exists()
    assert (tmp_path / "runtime/kr/watchlist" / trade_date.isoformat() / "prep_contract.json").exists()
    assert not legacy.exists()
