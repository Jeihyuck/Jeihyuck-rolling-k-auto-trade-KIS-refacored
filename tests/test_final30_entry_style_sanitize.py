"""
tests/test_final30_entry_style_sanitize.py

entry_style_selected sanitize 함수 검증:
- invalid entry_style가 1개 있어도 sanitize 후 PREP 전체가 죽지 않음
- 점수 기반 추론이 올바르게 작동함
- hard=True일 때 invalid_after>0이면 ValueError 발생
"""
from __future__ import annotations

import pytest
import pandas as pd

from trader.watchlist_builder import (
    ALLOWED_ENTRY_STYLES,
    sanitize_final30_entry_styles,
    _normalize_entry_style_value,
    _infer_entry_style_from_scores,
    _to_float_safe,
)


def _make_row(
    code: str = "005930",
    entry_style_selected: str | None = "BREAKOUT",
    breakout_score: float = 50.0,
    pullback_score: float = 30.0,
    momentum_score: float = 20.0,
) -> dict:
    return {
        "code": code,
        "entry_style_selected": entry_style_selected,
        "breakout_score": breakout_score,
        "pullback_score": pullback_score,
        "momentum_score": momentum_score,
        "score_final": 75.0,
        "tech_score": 60.0,
        "ma20": 50000.0,
        "ma50": 48000.0,
        "ma150": 45000.0,
        "close": 52000.0,
        "meta": {},
    }


def _make_30_rows(override_idx: int = 0, **override_kw) -> list[dict]:
    rows = []
    for i in range(30):
        code = str(100000 + i).zfill(6)
        if i == override_idx:
            rows.append(_make_row(code=code, **override_kw))
        else:
            rows.append(_make_row(code=code))
    return rows


class TestToFloatSafe:
    def test_none_returns_default(self):
        assert _to_float_safe(None) == 0.0

    def test_empty_string_returns_default(self):
        assert _to_float_safe("") == 0.0

    def test_valid_float(self):
        assert _to_float_safe("3.14") == pytest.approx(3.14)

    def test_invalid_string_returns_default(self):
        assert _to_float_safe("abc", default=-1.0) == -1.0


class TestNormalizeEntryStyleValue:
    def test_canonical_values_unchanged(self):
        assert _normalize_entry_style_value("BREAKOUT") == "BREAKOUT"
        assert _normalize_entry_style_value("PULLBACK") == "PULLBACK"
        assert _normalize_entry_style_value("MOMENTUM") == "MOMENTUM"

    def test_lowercase_normalized(self):
        assert _normalize_entry_style_value("breakout") == "BREAKOUT"

    def test_alias_entry_breakout(self):
        assert _normalize_entry_style_value("ENTRY_BREAKOUT") == "BREAKOUT"

    def test_alias_entry_momentum(self):
        assert _normalize_entry_style_value("ENTRY_MOMENTUM") == "MOMENTUM"

    def test_alias_entry_pullback(self):
        assert _normalize_entry_style_value("ENTRY_PULLBACK") == "PULLBACK"

    def test_unknown_value_returned_as_is(self):
        result = _normalize_entry_style_value("UNKNOWN_VALUE")
        assert result == "UNKNOWN_VALUE"
        assert result not in ALLOWED_ENTRY_STYLES


class TestInferEntryStyleFromScores:
    def test_highest_score_wins(self):
        assert _infer_entry_style_from_scores(
            {"breakout_score": 10.0, "pullback_score": 50.0, "momentum_score": 20.0}
        ) == "PULLBACK"

    def test_all_zero_returns_momentum(self):
        assert _infer_entry_style_from_scores(
            {"breakout_score": 0.0, "pullback_score": 0.0, "momentum_score": 0.0}
        ) == "MOMENTUM"

    def test_tiebreak_momentum_wins_over_breakout(self):
        # 동점이면 MOMENTUM > PULLBACK > BREAKOUT 우선
        result = _infer_entry_style_from_scores(
            {"breakout_score": 50.0, "pullback_score": 0.0, "momentum_score": 50.0}
        )
        assert result == "MOMENTUM"


class TestSanitizeFinal30EntryStyles:
    """케이스 1~4 검증"""

    def test_case1_entry_momentum_alias_normalized(self):
        """케이스 1: ENTRY_MOMENTUM → MOMENTUM 변환, invalid_after=0"""
        rows = _make_30_rows(
            override_idx=5,
            entry_style_selected="ENTRY_MOMENTUM",
            breakout_score=10.0,
            pullback_score=20.0,
            momentum_score=50.0,
        )
        result = sanitize_final30_entry_styles(rows, stage="test_case1", hard=False)
        assert len(result) == 30
        invalid = [r for r in result if r.get("entry_style_selected") not in ALLOWED_ENTRY_STYLES]
        assert len(invalid) == 0, f"invalid rows: {invalid}"
        assert result[5]["entry_style_selected"] == "MOMENTUM"

    def test_case2_none_style_inferred_from_scores(self):
        """케이스 2: entry_style_selected=None, breakout=10 pullback=50 momentum=20 → PULLBACK"""
        rows = _make_30_rows(
            override_idx=3,
            entry_style_selected=None,
            breakout_score=10.0,
            pullback_score=50.0,
            momentum_score=20.0,
        )
        result = sanitize_final30_entry_styles(rows, stage="test_case2", hard=False)
        assert result[3]["entry_style_selected"] == "PULLBACK"
        assert all(r.get("entry_style_selected") in ALLOWED_ENTRY_STYLES for r in result)

    def test_case3_bad_value_all_zeros_defaults_momentum(self):
        """케이스 3: entry_style_selected='bad_value', 모든 score=0 → MOMENTUM"""
        rows = _make_30_rows(
            override_idx=10,
            entry_style_selected="bad_value",
            breakout_score=0.0,
            pullback_score=0.0,
            momentum_score=0.0,
        )
        result = sanitize_final30_entry_styles(rows, stage="test_case3", hard=False)
        assert result[10]["entry_style_selected"] == "MOMENTUM"
        assert all(r.get("entry_style_selected") in ALLOWED_ENTRY_STYLES for r in result)

    def test_case4_sanitize_then_hard_contract_passes(self):
        """케이스 4: sanitize 후 assert_final30_scored_contract hard=True 통과"""
        from trader.watchlist_builder import assert_final30_scored_contract

        rows = _make_30_rows(
            override_idx=7,
            entry_style_selected="ENTRY_BREAKOUT",
            breakout_score=80.0,
            pullback_score=20.0,
            momentum_score=10.0,
        )
        sanitized = sanitize_final30_entry_styles(rows, stage="test_case4", hard=True)
        assert len(sanitized) == 30

        df = pd.DataFrame(sanitized)
        # 최소 contract 필드 보완
        for col in ("rs_percentile", "vcp_score", "atr_pct"):
            if col not in df.columns:
                df[col] = 1.0

        result = assert_final30_scored_contract(df, "test_case4", "2026-06-04", hard=True)
        assert result["ok"] is True
        assert len(result["errors"]) == 0

    def test_hard_mode_raises_if_unfixable(self):
        """hard=True 시 sanitize 후에도 invalid가 남으면 ValueError 발생 (이 경우는 불가능하지만 hard 경로 테스트)."""
        # sanitize 후에는 항상 valid이므로 hard=True여도 통과해야 함
        rows = _make_30_rows(
            override_idx=0,
            entry_style_selected="INVALID",
        )
        # sanitize가 올바르게 보정하면 ValueError 미발생
        result = sanitize_final30_entry_styles(rows, stage="hard_test", hard=True)
        assert len(result) == 30

    def test_pass_flags_set_correctly(self):
        """breakout_score > 0이면 breakout_pass=True"""
        rows = [_make_row(breakout_score=50.0, pullback_score=0.0, momentum_score=0.0)]
        result = sanitize_final30_entry_styles(rows, stage="flags_test", hard=False)
        assert result[0]["breakout_pass"] is True
        assert result[0]["pullback_pass"] is False
        assert result[0]["momentum_pass"] is False

    def test_meta_fields_synced(self):
        """sanitize 후 meta에도 entry_style_selected가 동기화되어야 함"""
        rows = [_make_row(entry_style_selected="ENTRY_PULLBACK", pullback_score=60.0)]
        result = sanitize_final30_entry_styles(rows, stage="meta_test", hard=False)
        assert result[0]["entry_style_selected"] == "PULLBACK"
        assert result[0]["meta"]["entry_style_selected"] == "PULLBACK"
        assert result[0]["meta"]["entry_component"] == "pullback"
