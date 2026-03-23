from __future__ import annotations

from trader.final30_quality import verify_final30_scored_rows


def test_monoculture_false_positive_is_avoided_for_realistic_distribution() -> None:
    rows = []
    for idx in range(30):
        rows.append(
            {
                "code": f"{idx + 1:06d}",
                "close": 100.0 + idx,
                "ma20": 95.0 + idx,
                "ma50": 90.0 + idx,
                "ma150": 80.0 + idx,
                "atr_pct": 0.02 + (idx % 3) * 0.001,
                "rs_percentile": 80.0 + (idx % 5),
                "breakout_score": 5.0 if idx < 3 else 8.0,
                "pullback_score": 55.0 + (idx % 4),
                "momentum_score": 65.0 + (idx % 6),
                "tech_score": 70.0 + (idx % 7),
                "score_final": 80.0 + (idx * 0.1),
                "entry_style_selected": "PULLBACK" if idx % 4 else "MOMENTUM",
            }
        )

    result = verify_final30_scored_rows(
        rows,
        required_rows=30,
        required_fields=["code", "ma20", "atr_pct", "score_final", "breakout_score", "pullback_score", "momentum_score", "entry_style_selected"],
        source="test",
    )

    assert result["momentum_monoculture"] is False
    assert result["score_monoculture"] is False
    assert result["ok"] is True