from __future__ import annotations

from trader.watchlist_builder import _build_final30_saved_rows


def test_final30_saved_rows_preserves_flow_provenance_fields() -> None:
    rows = [
        {
            "code": "005930",
            "rank": 1,
            "score": 99.0,
            "score_final": 99.0,
            "tech_score": 70.0,
            "breakout_score": 20.0,
            "pullback_score": 30.0,
            "momentum_score": 40.0,
            "entry_style_selected": "PULLBACK",
            "close": 100.0,
            "ma20": 98.0,
            "ma50": 95.0,
            "ma150": 90.0,
            "atr_pct": 0.03,
            "flow_data_available": 0,
            "flow_provider_used": "none",
            "flow_fail_reason": "kis:init_failed;pykrx:import_failed",
            "flow_score_imputed": 1,
            "foreign_flow_missing": 1,
            "inst_flow_missing": 1,
        }
    ]

    saved = _build_final30_saved_rows(rows)

    assert len(saved) == 1
    assert saved[0]["flow_data_available"] == 0
    assert saved[0]["flow_provider_used"] == "none"
    assert saved[0]["flow_fail_reason"]
    assert saved[0]["flow_score_imputed"] == 1
    assert saved[0]["foreign_flow_missing"] == 1
    assert saved[0]["inst_flow_missing"] == 1
