from __future__ import annotations

import json
from datetime import datetime, time, timedelta
from pathlib import Path

import pytest

from trader.kr import artifacts
from trader.kr.calendar import resolve_kr_expected_as_of
from trader.kr.runner.session_policy import KST, wait_until_kr_am_target


def _rows(as_of):
    return [{"code": f"{i:06d}", "as_of": as_of.isoformat(), "score_final": 30 - i} for i in range(30)]


def test_publish_validate_and_stale_latest_reject(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    trade_date = datetime.now(KST).date()
    while trade_date.weekday() >= 5:
        trade_date += timedelta(days=1)
    expected = resolve_kr_expected_as_of(trade_date)

    artifacts.publish_kr_prep_artifacts_atomic(
        trade_date=trade_date,
        expected_as_of=expected,
        actual_as_of=expected,
        env="practice",
        final30_rows=_rows(expected),
        db_exact_rows=30,
        metadata={},
    )
    ok = artifacts.validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected, env="practice")
    assert ok.ok is True

    contract = json.loads((tmp_path / "signals/kr/latest_prep_contract.json").read_text())
    contract["expected_as_of"] = (expected - timedelta(days=1)).isoformat()
    (tmp_path / "signals/kr/latest_prep_contract.json").write_text(json.dumps(contract), encoding="utf-8")
    rejected = artifacts.validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected, env="practice")
    assert rejected.ok is False
    assert rejected.reason == "ASOF_MISMATCH"


def test_legacy_blocks_and_quarantine_moves(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    trade_date = datetime.now(KST).date()
    while trade_date.weekday() >= 5:
        trade_date += timedelta(days=1)
    expected = resolve_kr_expected_as_of(trade_date)
    legacy = tmp_path / "signals/final30.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text(json.dumps({"as_of": (expected - timedelta(days=1)).isoformat(), "rows": _rows(expected)}), encoding="utf-8")

    result = artifacts.validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected, env="practice")
    assert result.ok is False
    assert result.reason == "CANONICAL_MISSING"

    assert not legacy.exists()


def test_am_wait_reaches_target_without_truncation(monkeypatch):
    trade_date = datetime.now(KST).date()
    target = datetime.combine(trade_date, time(9, 0, 5), tzinfo=KST)
    calls = [target - timedelta(seconds=1), target]

    def now_fn():
        return calls.pop(0) if calls else target

    monkeypatch.setattr("trader.kr.runner.session_policy.time_mod.sleep", lambda _s: None)
    wait_until_kr_am_target(trade_date=trade_date, target_time=target.time(), now_fn=now_fn, max_wait_sec=5)
