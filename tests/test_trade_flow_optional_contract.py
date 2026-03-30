from __future__ import annotations

from trader import pb1_runner


def test_trade_required_cols_ignore_flow_columns() -> None:
    cols = [
        "code",
        "name",
        "score_final",
        "tech_score",
        "breakout_score",
        "pullback_score",
        "momentum_score",
        "rs_percentile",
        "vcp_score",
        "entry_style_selected",
        "close",
        "ma20",
        "ma50",
        "ma150",
        "atr_pct",
    ]
    missing = pb1_runner._missing_scored_cols(cols)
    assert missing == []


def test_trade_required_cols_accept_last_close_as_alternative() -> None:
    cols = [
        "code",
        "name",
        "score_final",
        "tech_score",
        "breakout_score",
        "pullback_score",
        "momentum_score",
        "rs_percentile",
        "vcp_score",
        "entry_style_selected",
        "last_close",
        "ma20",
        "ma50",
        "ma150",
        "atr_pct",
    ]
    missing = pb1_runner._missing_scored_cols(cols)
    assert missing == []
