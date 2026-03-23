from __future__ import annotations

import pandas as pd
import pytest

from trader.exporter import _normalize_record
from trader.final30_quality import summarize_final30_quality, validate_trade_ready
from trader.minervini.compute import _compute_entry_scores


def _base_final30_rows() -> list[dict]:
    return [
        {
            "code": f"{idx + 1:06d}",
            "ma20": 100.0 + idx,
            "atr_pct": 0.03,
            "breakout_score": 50.0 + idx,
            "pullback_score": 40.0 + idx,
            "momentum_score": 60.0 + idx,
            "entry_style_selected": "BREAKOUT" if idx % 3 == 0 else "PULLBACK",
            "score_final": 80.0 + idx,
        }
        for idx in range(30)
    ]


def test_derived_entry_flat_placeholder_is_rejected() -> None:
    breakout, pullback, momentum, context = _compute_entry_scores(
        "005930",
        {
            "close": 100.0,
            "pivot": 100.0,
            "hi_52w": 100.0,
            "latest_volume": 100.0,
            "vol20": 100.0,
            "atr_pct": 0.02,
            "ma20": 95.0,
            "ma50": 90.0,
            "ma150": 85.0,
        },
        {"close": 100.0, "volume": 100.0, "high": 101.0, "low": 99.0},
        82.0,
    )

    assert breakout is None
    assert pullback is None
    assert momentum is None
    assert context["entry_invalid_reason"] == "invalid_entry_inputs"


def test_final30_quality_contract_fails_when_ma20_zero() -> None:
    rows = _base_final30_rows()
    for row in rows:
        row["ma20"] = 0.0
    summary = summarize_final30_quality(pd.DataFrame(rows))

    assert summary["ok"] is False
    assert summary["valid_ma20_ratio"] == 0.0


def test_monoculture_detection_fails_constant_breakout_scores() -> None:
    rows = _base_final30_rows()
    for row in rows:
        row["breakout_score"] = 45.0
        row["pullback_score"] = 42.0
        row["momentum_score"] = 60.0
        row["entry_style_selected"] = "MOMENTUM"
    summary = summarize_final30_quality(pd.DataFrame(rows))

    assert summary["ok"] is False
    assert summary["entry_style_monoculture"] is True
    assert summary["score_monoculture"] is True


def test_trade_ready_validation_aborts_on_invalid_atr_pct() -> None:
    rows = _base_final30_rows()
    rows[5]["atr_pct"] = 0.0

    with pytest.raises(RuntimeError, match=r"TRADE\]\[ABORT\]\[FINAL30_INVALID"):
        validate_trade_ready(pd.DataFrame(rows))


def test_alias_consistency_preserves_score_final_semantics() -> None:
    normalized = _normalize_record(
        {
            "code": "000001",
            "name": "Test",
            "score_final": 91.5,
            "final_score": 91.5,
            "tech_score": 81.5,
            "breakout_score": 61.0,
            "pullback_score": 51.0,
            "momentum_score": 71.0,
            "entry_style_selected": "BREAKOUT",
            "rank": 1,
        }
    )

    assert normalized["score_final"] == 91.5
    assert normalized["final_score"] == 91.5
    assert normalized["entry_style_selected"] == "BREAKOUT"