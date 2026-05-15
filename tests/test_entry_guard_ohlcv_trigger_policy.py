from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.pb1_engine import PB1Engine, CandidateFeature


def _make_kr_pb1_final30_cf(code: str = "028050", missing_feature: str | None = None) -> CandidateFeature:
    """KR PB1 final30 경로 CandidateFeature 빌더 (테스트용)."""
    features: dict = {
        "close": 75000.0,
        "ma20": 72000.0,
        "ma50": 68000.0,
        "ma150": 62000.0,
        "atr_pct": 2.5,
        "rs_percentile": 85.0,
        "vcp_score": 3.0,
        "breakout_score": 2.0,
        "pullback_score": 1.5,
        "momentum_score": 2.8,
        "entry_style_selected": "BREAKOUT",
    }
    if missing_feature is not None:
        features.pop(missing_feature, None)
    return CandidateFeature(
        code=code,
        market="KR",
        features=features,
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=["pb1_from_final30"],
        planned_qty=10,
    )


def test_entry_trigger_policy_marks_setup_override() -> None:
    policy = PB1Engine._resolve_entry_trigger_policy(
        trigger_ok=False,
        entry_ok=True,
        setup_filters_ok=True,
        decision_family="ENTRY_SETUP_OVERRIDE",
    )

    assert policy == "SETUP_OVERRIDE"


def test_explicit_trigger_bypass_blocks_none_policy(monkeypatch) -> None:
    monkeypatch.setenv("PB1_REQUIRE_EXPLICIT_TRIGGER_BYPASS", "1")

    entry_ok, reasons = PB1Engine._enforce_explicit_trigger_bypass(
        entry_ok=True,
        trigger_ok=False,
        trigger_policy="NONE",
        reasons=[],
    )

    assert entry_ok is False
    assert reasons == ["explicit_trigger_bypass_required"]


def test_entry_ohlcv_guard_blocks_insufficient_rows(monkeypatch) -> None:
    monkeypatch.setenv("PB1_BLOCK_INSUFFICIENT_OHLCV", "1")
    df = pd.DataFrame([{"date": f"2026-05-{idx:02d}", "close": 1} for idx in range(1, 60)])

    reason = PB1Engine._entry_ohlcv_block_reason(
        df=df,
        meta={"source": "db_short_only", "long_fetch_blocked": 1},
    )

    assert reason == "insufficient_ohlcv"


def test_entry_ohlcv_guard_allows_sufficient_history(monkeypatch) -> None:
    monkeypatch.setenv("PB1_BLOCK_INSUFFICIENT_OHLCV", "1")
    df = pd.DataFrame([{"date": f"2026-05-{idx:02d}", "close": 1} for idx in range(1, 130)])

    reason = PB1Engine._entry_ohlcv_block_reason(
        df=df,
        meta={"source": "db", "long_fetch_blocked": 0},
    )

    assert reason is None


# ── KR PB1 final30 precomputed 완화 경로 테스트 ──────────────────────────────

def test_kr_pb1_precomputed_60_rows_allows_entry(monkeypatch) -> None:
    """KR PB1 final30 + precomputed 필수 feature 모두 OK + rows=60 → 통과 (None)."""
    monkeypatch.setenv("PB1_BLOCK_INSUFFICIENT_OHLCV", "1")
    df = pd.DataFrame([{"date": f"2026-05-{idx:02d}", "close": 75000} for idx in range(1, 61)])  # rows=60
    cf = _make_kr_pb1_final30_cf(code="028050")

    reason = PB1Engine._entry_ohlcv_block_reason(
        df=df,
        meta={"source": "db_short_only", "long_fetch_blocked": 1},
        cf=cf,
    )

    assert reason is None


def test_kr_pb1_precomputed_59_rows_blocks_entry(monkeypatch) -> None:
    """rows=59 → 신규상장 방어 차단 (insufficient_ohlcv)."""
    monkeypatch.setenv("PB1_BLOCK_INSUFFICIENT_OHLCV", "1")
    df = pd.DataFrame([{"date": f"2026-05-{idx:02d}", "close": 75000} for idx in range(1, 60)])  # rows=59
    cf = _make_kr_pb1_final30_cf(code="028050")

    reason = PB1Engine._entry_ohlcv_block_reason(
        df=df,
        meta={"source": "db_short_only", "long_fetch_blocked": 1},
        cf=cf,
    )

    assert reason == "insufficient_ohlcv"


def test_kr_pb1_missing_precomputed_feature_blocks_entry(monkeypatch) -> None:
    """필수 feature 중 하나라도 없으면 차단."""
    monkeypatch.setenv("PB1_BLOCK_INSUFFICIENT_OHLCV", "1")
    df = pd.DataFrame([{"date": f"2026-05-{idx:02d}", "close": 75000} for idx in range(1, 61)])
    cf = _make_kr_pb1_final30_cf(code="028050", missing_feature="atr_pct")

    reason = PB1Engine._entry_ohlcv_block_reason(
        df=df,
        meta={"source": "db_short_only", "long_fetch_blocked": 1},
        cf=cf,
    )

    assert reason == "insufficient_ohlcv"


def test_us_pb1_short_ohlcv_still_blocks_entry(monkeypatch) -> None:
    """US 코드(비 KR)는 precomputed feature가 있어도 기존 정책 유지 (차단)."""
    monkeypatch.setenv("PB1_BLOCK_INSUFFICIENT_OHLCV", "1")
    df = pd.DataFrame([{"date": f"2026-05-{idx:02d}", "close": 150.0} for idx in range(1, 61)])
    # US 코드: 6자리 숫자가 아님
    cf_us = CandidateFeature(
        code="AAPL",
        market="US",
        features={
            "close": 150.0, "ma20": 148.0, "ma50": 145.0, "ma150": 140.0,
            "atr_pct": 1.8, "rs_percentile": 80.0, "vcp_score": 2.5,
            "breakout_score": 2.0, "pullback_score": 1.0, "momentum_score": 2.2,
            "entry_style_selected": "BREAKOUT",
        },
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=["pb1_from_final30"],
        planned_qty=5,
    )

    reason = PB1Engine._entry_ohlcv_block_reason(
        df=df,
        meta={"source": "db_short_only", "long_fetch_blocked": 1},
        cf=cf_us,
    )

    assert reason == "insufficient_ohlcv"


def test_general_strategy_short_ohlcv_still_blocks_entry(monkeypatch) -> None:
    """KR 코드라도 final30 경로가 아니면 기존 정책 유지 (차단)."""
    monkeypatch.setenv("PB1_BLOCK_INSUFFICIENT_OHLCV", "1")
    df = pd.DataFrame([{"date": f"2026-05-{idx:02d}", "close": 75000} for idx in range(1, 61)])
    cf_general = CandidateFeature(
        code="005930",
        market="KR",
        features={
            "close": 75000.0, "ma20": 72000.0, "ma50": 68000.0, "ma150": 62000.0,
            "atr_pct": 2.5, "rs_percentile": 85.0, "vcp_score": 3.0,
            "breakout_score": 2.0, "pullback_score": 1.5, "momentum_score": 2.8,
            "entry_style_selected": "BREAKOUT",
        },
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=["default_day_mode"],  # pb1_from_final30 아님
        planned_qty=10,
    )

    reason = PB1Engine._entry_ohlcv_block_reason(
        df=df,
        meta={"source": "db_short_only", "long_fetch_blocked": 1},
        cf=cf_general,
    )

    assert reason == "insufficient_ohlcv"


def test_kr_pb1_precomputed_no_long_block_still_proceeds(monkeypatch) -> None:
    """long_fetch_blocked=0이면 KR PB1 precomputed bypass 적용 안 함.

    source='db_short_only'지만 long_fetch_blocked=False → source 체크에서 차단.
    """
    monkeypatch.setenv("PB1_BLOCK_INSUFFICIENT_OHLCV", "1")
    df = pd.DataFrame([{"date": f"2026-05-{idx:02d}", "close": 75000} for idx in range(1, 61)])
    cf = _make_kr_pb1_final30_cf(code="028050")

    reason = PB1Engine._entry_ohlcv_block_reason(
        df=df,
        meta={"source": "db_short_only", "long_fetch_blocked": 0},
        cf=cf,
    )

    # long_fetch_blocked=0 이므로 bypass 불가, source=db_short_only 로 차단
    assert reason == "insufficient_ohlcv"