"""Regression tests for PREP final30 export consistency alias handling."""

from __future__ import annotations

import pandas as pd

from trader.prep_runner import _assert_same_nonzero, _collect_final30_nonzero_stats


def test_prep_final30_stats_alias_primary_columns():
    df = pd.DataFrame(
        [
            {
                "code": "000001",
                "tech_score": 10.0,
                "score_final": 20.0,
                "breakout_score": 30.0,
                "pullback_score": 40.0,
                "momentum_score": 50.0,
            },
            {
                "code": "000002",
                "tech_score": 11.0,
                "score_final": 21.0,
                "breakout_score": 31.0,
                "pullback_score": 41.0,
                "momentum_score": 51.0,
            },
        ]
    )

    stats, _ = _collect_final30_nonzero_stats(df)
    assert stats["tech_nonzero"] == 2
    assert stats["score_final_nonzero"] == 2
    assert stats["breakout_nonzero"] == 2
    assert stats["pullback_nonzero"] == 2
    assert stats["momentum_nonzero"] == 2


def test_prep_final30_stats_alias_fallback_columns():
    df = pd.DataFrame(
        [
            {
                "code": "000001",
                "final_score": 20.0,
                "score_breakout": 30.0,
                "score_pullback": 40.0,
                "score_momentum": 50.0,
            },
            {
                "code": "000002",
                "final_score": 22.0,
                "score_breakout": 32.0,
                "score_pullback": 42.0,
                "score_momentum": 52.0,
            },
        ]
    )

    stats, _ = _collect_final30_nonzero_stats(df)
    assert stats["score_final_nonzero"] == 2
    assert stats["breakout_nonzero"] == 2
    assert stats["pullback_nonzero"] == 2
    assert stats["momentum_nonzero"] == 2


def test_final30_consistency_compare_with_alias_mismatch_schema():
    lhs_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "tech_score": 10.0,
                "score_final": 20.0,
                "breakout_score": 30.0,
                "pullback_score": 40.0,
                "momentum_score": 50.0,
            }
        ]
    )
    rhs_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "tech": 10.0,
                "final_score": 20.0,
                "score_breakout": 30.0,
                "score_pullback": 40.0,
                "score_momentum": 50.0,
            }
        ]
    )

    lhs_stats, _ = _collect_final30_nonzero_stats(lhs_df)
    rhs_stats, _ = _collect_final30_nonzero_stats(rhs_df)

    _assert_same_nonzero("final30_tech", lhs_stats["tech_nonzero"], rhs_stats["tech_nonzero"])
    _assert_same_nonzero("final30_score_final", lhs_stats["score_final_nonzero"], rhs_stats["score_final_nonzero"])
    _assert_same_nonzero("final30_breakout", lhs_stats["breakout_nonzero"], rhs_stats["breakout_nonzero"])
    _assert_same_nonzero("final30_pullback", lhs_stats["pullback_nonzero"], rhs_stats["pullback_nonzero"])
    _assert_same_nonzero("final30_momentum", lhs_stats["momentum_nonzero"], rhs_stats["momentum_nonzero"])


def test_pb1_final30_export_regression_counts_30():
    rows = []
    for idx in range(30):
        rows.append(
            {
                "code": f"{idx + 1:06d}",
                "tech_score": 70.0 + idx,
                "score_final": 60.0 + idx,
                "breakout_score": 50.0 + idx,
                "pullback_score": 40.0 + idx,
                "momentum_score": 30.0 + idx,
            }
        )

    df = pd.DataFrame(rows)
    stats, _ = _collect_final30_nonzero_stats(df)

    assert stats["tech_nonzero"] == 30
    assert stats["score_final_nonzero"] == 30
    assert stats["breakout_nonzero"] == 30
    assert stats["pullback_nonzero"] == 30
    assert stats["momentum_nonzero"] == 30
