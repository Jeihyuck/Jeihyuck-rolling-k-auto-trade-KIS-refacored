from __future__ import annotations

from datetime import date

import pytest

from trader.kr import artifacts
from trader.kr.artifacts import KrArtifactValidationResult
from trader.kr.runner import trade_session_runner as runner


def _rows(as_of: date, n: int = 30):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "code": f"{i:06d}",
            "as_of": as_of.isoformat(),
            "rank_final30": i,
            "score_final": 10 + i,
            "tech_score": 10 + i,
            "breakout_score": 1,
            "pullback_score": 1,
            "momentum_score": 1,
            "entry_style_selected": "BREAKOUT",
            "ma20": 100,
            "ma50": 90,
            "ma150": 80,
            "rs_percentile": 90,
            "vcp_score": 1,
            "atr_pct": 2,
            "close": 1000,
        })
    return rows


def test_publish_core_fast_does_not_call_load_exact_final30_scored(tmp_path, monkeypatch):
    trade_date = date(2026, 6, 26)
    expected = date(2026, 6, 25)
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    monkeypatch.setattr(artifacts, "load_exact_final30_scored", lambda *a, **k: pytest.fail("core fast path must not read DB"))

    res = artifacts.publish_kr_prep_artifacts_core_fast(
        trade_date=trade_date,
        expected_as_of=expected,
        actual_as_of=expected,
        env="practice",
        final30_rows=_rows(expected),
        db_exact_rows=30,
        metadata={"source": "test"},
    )

    assert res.ok
    assert (tmp_path / "runtime/kr/watchlist/2026-06-26/prep_contract.json").exists()
    assert (tmp_path / "runtime/kr/watchlist/2026-06-26/final30_scored.json").exists()
    assert (tmp_path / "runtime/kr/watchlist/2026-06-26/prep_done.json").exists()
    assert (tmp_path / "runtime/kr/prep_status/2026-06-26/prep_status.json").exists()
    assert (tmp_path / "signals/kr/latest_prep_contract.json").exists()
    assert (tmp_path / "signals/kr/latest_prep_status.json").exists()


def test_artifact_post_verify_timeout_does_not_fail_core(tmp_path, monkeypatch):
    trade_date = date(2026, 6, 26)
    expected = date(2026, 6, 25)
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    monkeypatch.setattr(artifacts, "validate_kr_prep_artifact_db_exact", lambda **kw: (_ for _ in ()).throw(TimeoutError("boom")))

    res = artifacts.publish_kr_prep_artifacts_core_fast(
        trade_date=trade_date,
        expected_as_of=expected,
        actual_as_of=expected,
        env="practice",
        final30_rows=_rows(expected),
        db_exact_rows=30,
        require_db_exact=True,
    )

    assert res.ok
    assert (tmp_path / "signals/kr/latest_prep_contract.json").exists()


def test_session_precheck_rescue_from_db_rebuilds_missing_artifact(tmp_path, monkeypatch, caplog):
    trade_date = date(2026, 6, 26)
    expected = date(2026, 6, 25)
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(artifacts, "load_exact_final30_scored", lambda *a, **k: _rows(expected))
    monkeypatch.setattr(artifacts, "get_engine", lambda: object())
    ctx = runner.KrSessionContext("afternoon", trade_date, expected, "practice", "KR", runner._now_kst(), False, None, None, False)

    guarded = runner._guard_trade_session("afternoon", ctx)

    assert guarded is None
    assert (tmp_path / "signals/kr/latest_prep_contract.json").exists()
    assert "[KR_SESSION][PRECHECK_RESCUED]" in caplog.text


def test_session_precheck_blocks_when_contract_missing_and_db_invalid(tmp_path, monkeypatch):
    trade_date = date(2026, 6, 26)
    expected = date(2026, 6, 25)
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(artifacts, "load_exact_final30_scored", lambda *a, **k: _rows(expected, 29))
    monkeypatch.setattr(artifacts, "get_engine", lambda: object())
    ctx = runner.KrSessionContext("am", trade_date, expected, "practice", "KR", runner._now_kst(), False, None, None, False)

    guarded = runner._guard_trade_session("afternoon", ctx)

    assert guarded is not None
    assert guarded["status"] == "FAIL"
