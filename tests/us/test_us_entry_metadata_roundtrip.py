import pytest

from trader.us.pb1.us_entry_engine import _resolve_entry_signal_type
from trader.us.pb1.us_explain import build_us_entry_explanation, normalize_us_entry_style


def test_jnj_pb1_metadata_and_scores_survive_explanation():
    row = {"symbol": "JNJ", "entry_style_selected": "pb1_pullback",
           "momentum_score": .4167, "pullback_score": .9, "breakout_score": .4236,
           "vcp_score": 1.0, "score_final": .5785, "rank_final30": 16,
           "trend_score": 1.0, "theme_cluster": "HEALTHCARE"}
    assert normalize_us_entry_style(row["entry_style_selected"]) == "ENTRY_PULLBACK"
    assert _resolve_entry_signal_type(row) == "pullback"
    explanation = build_us_entry_explanation("JNJ", row, "BUY")
    assert explanation["entry_style_selected"] == "ENTRY_PULLBACK"
    assert explanation["score_breakdown"]["pullback_score"] == pytest.approx(.9)
    assert explanation["score_breakdown"]["momentum_score"] == pytest.approx(.4167)
