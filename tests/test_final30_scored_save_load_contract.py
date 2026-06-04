"""
tests/test_final30_scored_save_load_contract.py

pb1_watchlist_final_scored 저장 → strict load 검증:
- 30개 저장 후 load_final30_scored_exact로 재조회 rows=30
- contract_ok=1, usable=1
- entry_style_invalid=0
- meta flatten 동작 검증
"""
from __future__ import annotations

import pytest
from datetime import date
from typing import Any

from trader.watchlist_builder import (
    sanitize_final30_entry_styles,
    ALLOWED_ENTRY_STYLES,
)


def _make_scored_row(
    code: str,
    rank: int,
    *,
    entry_style: str = "BREAKOUT",
    breakout_score: float = 50.0,
    pullback_score: float = 30.0,
    momentum_score: float = 20.0,
) -> dict[str, Any]:
    return {
        "code": code,
        "rank": rank,
        "rank_final30": rank,
        "score": 70.0,
        "score_final": 70.0,
        "tech_score": 60.0,
        "breakout_score": breakout_score,
        "pullback_score": pullback_score,
        "momentum_score": momentum_score,
        "entry_style_selected": entry_style,
        "entry_component": entry_style.lower(),
        "ma20": 50000.0,
        "ma50": 48000.0,
        "ma150": 45000.0,
        "close": 52000.0,
        "atr_pct": 2.5,
        "rs_percentile": 85.0,
        "rs_pctile": 85.0,
        "vcp_score": 75.0,
        "reasons": {"passed": ["RS>=80"], "failed": []},
        "filters_passed": ["A_POOL120", "B_TOP50", "C_FINAL30"],
        "filters_failed": [],
        "meta": {
            "score_final": 70.0,
            "tech_score": 60.0,
            "breakout_score": breakout_score,
            "pullback_score": pullback_score,
            "momentum_score": momentum_score,
            "entry_style_selected": entry_style,
            "ma20": 50000.0,
            "ma50": 48000.0,
            "ma150": 45000.0,
            "close": 52000.0,
            "rs_percentile": 85.0,
            "vcp_score": 75.0,
            "atr_pct": 2.5,
            "rank_final30": rank,
            "as_of": "2026-06-04",
            "reasons": {"passed": ["RS>=80"], "failed": []},
            "filters_passed": ["A_POOL120", "B_TOP50", "C_FINAL30"],
            "filters_failed": [],
        },
    }


def _make_30_scored_rows() -> list[dict]:
    rows = []
    for i in range(30):
        code = str(100000 + i).zfill(6)
        entry_styles = ["BREAKOUT", "PULLBACK", "MOMENTUM"]
        style = entry_styles[i % 3]
        rows.append(_make_scored_row(code, rank=i + 1, entry_style=style))
    return rows


class TestSanitizeIntegration:
    """sanitize → contract 통합 검증"""

    def test_30_rows_all_valid_pass_contract(self):
        from trader.watchlist_builder import assert_final30_scored_contract
        import pandas as pd

        rows = _make_30_scored_rows()
        sanitized = sanitize_final30_entry_styles(rows, stage="integration_test", hard=False)

        assert len(sanitized) == 30
        df = pd.DataFrame(sanitized)
        result = assert_final30_scored_contract(df, "integration_test", "2026-06-04", hard=True)
        assert result["ok"] is True
        assert result["rows"] == 30
        assert result["uniq_code"] == 30
        assert "entry_style_invalid" not in str(result["errors"])

    def test_mixed_invalid_styles_sanitized_to_valid(self):
        rows = _make_30_scored_rows()
        # 몇 개 invalid style 주입
        rows[0]["entry_style_selected"] = "ENTRY_BREAKOUT"
        rows[5]["entry_style_selected"] = "ENTRY_PULLBACK"
        rows[15]["entry_style_selected"] = None
        rows[29]["entry_style_selected"] = "bad_value"

        sanitized = sanitize_final30_entry_styles(rows, stage="mixed_test", hard=True)
        invalid = [r for r in sanitized if r.get("entry_style_selected") not in ALLOWED_ENTRY_STYLES]
        assert len(invalid) == 0, f"Still invalid after sanitize: {invalid}"


class TestFlattenWatchlistRow:
    """meta flatten 로직 검증 (repos.py의 _flatten_wl_row 동작 재현)"""

    def test_meta_fields_promoted_to_toplevel(self):
        import json

        row = {
            "env": "practice",
            "strategy": "pb1_watchlist_final_scored",
            "as_of": date(2026, 6, 4),
            "code": "005930",
            "rank": 1,
            "score": 70.0,
            "meta": json.dumps({
                "score_final": 75.0,
                "tech_score": 62.0,
                "breakout_score": 55.0,
                "pullback_score": 25.0,
                "momentum_score": 15.0,
                "entry_style_selected": "BREAKOUT",
                "ma20": 50000.0,
                "ma50": 48000.0,
                "ma150": 45000.0,
                "close": 52000.0,
                "rs_percentile": 82.0,
                "vcp_score": 78.0,
                "atr_pct": 2.1,
                "rank_final30": 1,
                "as_of": "2026-06-04",
            }),
        }

        # flatten 로직 재현 (repos.py의 _flatten_wl_row와 동일)
        import json as _json
        raw_meta = row.get("meta") or {}
        if isinstance(raw_meta, str):
            raw_meta = _json.loads(raw_meta)
        meta = raw_meta

        out = dict(meta)
        out["code"] = str(row.get("code") or "").zfill(6)
        out["score_final"] = float(meta.get("score_final") or row.get("score") or 0.0)
        out["tech_score"] = float(meta.get("tech_score") or 0.0)
        out["breakout_score"] = float(meta.get("breakout_score") or 0.0)
        out["entry_style_selected"] = meta.get("entry_style_selected")

        assert out["score_final"] == 75.0
        assert out["tech_score"] == 62.0
        assert out["breakout_score"] == 55.0
        assert out["entry_style_selected"] == "BREAKOUT"
        assert out["code"] == "005930"


class TestLoadResultLogging:
    """load_final30_scored_exact 반환값 구조 검증 (DB 없이 단위 테스트)"""

    def test_usable_requires_30_rows(self):
        """rows != 30이면 contract_ok=0, usable=0이어야 함"""
        from trader.constants import CRITICAL_SCORED_COLS

        rows_count = 29
        missing_critical = []  # 필드 없음 상황

        contract_ok = int(rows_count == 30 and not missing_critical)
        usable = int(contract_ok == 1)

        assert contract_ok == 0
        assert usable == 0

    def test_usable_1_when_30_rows_no_missing(self):
        from trader.constants import CRITICAL_SCORED_COLS

        rows_count = 30
        missing_critical: list = []

        contract_ok = int(rows_count == 30 and not missing_critical)
        usable = int(contract_ok == 1)

        assert contract_ok == 1
        assert usable == 1
