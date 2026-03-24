from __future__ import annotations

import logging

import pandas as pd

from trader.watchlist_builder import assert_final30_scored_contract, normalize_ma20_column


def _base_rows() -> list[dict]:
    rows: list[dict] = []
    for idx in range(30):
        code = f"{idx + 1:06d}"
        rows.append(
            {
                "as_of": "2026-03-24",
                "code": code,
                "name": f"N{code}",
                "rank": idx + 1,
                "rank_pool120": idx + 1,
                "rank_top50": idx + 1,
                "rank_final30": idx + 1,
                "score": 80.0 + idx,
                "score_final": 80.0 + idx,
                "score_flow": 10.0,
                "score_liq": 10.0,
                "score_tech": 70.0 + idx,
                "tech_score": 70.0 + idx,
                "flow_score": 10.0,
                "final_score": 80.0 + idx,
                "breakout_score": 20.0 + (idx % 5),
                "pullback_score": 30.0 + (idx % 7),
                "momentum_score": 40.0 + (idx % 9),
                "entry_style_selected": "BREAKOUT",
                "entry_component": "breakout",
                "rs_pctile": 85.0,
                "rs_percentile": 85.0,
                "rs_score": 85.0,
                "vcp_score": 72.0,
                "trend_score": 75.0,
                "atr_pct": 0.03,
                "pullback_pct": 0.02,
                "foreign_20_ratio": 0.1,
                "inst_20_ratio": 0.2,
                "liq_avg": 1_000_000.0,
                "last_close": 120.0 + idx,
                "close": 120.0 + idx,
                "volume": 100_000.0,
                "volume_avg20": 90_000.0,
                "ma20": 110.0 + idx,
                "ma50": 105.0 + idx,
                "ma150": 95.0 + idx,
                "rows": 220,
                "meta": {"source": "test"},
                "scores": {"final": 80.0 + idx},
                "reasons": {"passed": ["ok"], "failed": []},
                "reject_reasons": [],
                "filters_passed": ["minervini"],
                "filters_failed": [],
            }
        )
    return rows


def test_normalize_ma20_column_recovers_from_alias_column() -> None:
    df = pd.DataFrame(_base_rows())
    df["ma20"] = None
    df["ma_20"] = [200.0 + idx for idx in range(len(df))]

    normalized = normalize_ma20_column(df, "test_alias_recover")
    result = assert_final30_scored_contract(normalized, "alias_recover", "2026-03-24", hard=False)

    assert int(normalized["ma20"].isna().sum()) == 0
    assert normalized["ma20"].tolist() == df["ma_20"].tolist()
    assert result["ok"] is True


def test_normalize_ma20_column_recovers_from_suffix_columns() -> None:
    df = pd.DataFrame(_base_rows())
    df = df.drop(columns=["ma20"])
    df["ma20_x"] = None
    df["ma20_y"] = [210.0 + idx for idx in range(len(df))]

    normalized = normalize_ma20_column(df, "test_suffix_recover")

    assert "ma20" in normalized.columns
    assert int(normalized["ma20"].isna().sum()) == 0
    assert normalized["ma20"].tolist() == df["ma20_y"].tolist()


def test_normalize_ma20_column_keeps_fail_when_all_candidates_missing(caplog) -> None:
    df = pd.DataFrame(_base_rows())
    for column in ("ma20", "ma_20", "sma20", "close_ma20", "moving_avg20", "avg20"):
        df[column] = None

    caplog.set_level(logging.INFO)
    normalized = normalize_ma20_column(df, "test_all_missing")
    result = assert_final30_scored_contract(normalized, "missing_source", "2026-03-24", hard=False)

    assert int(normalized["ma20"].isna().sum()) == 30
    assert result["ok"] is False
    assert "ma20_null count=30" in result["errors"]
    assert "[DEBUG][MA20_DIAG] stage=contract_missing_source" in caplog.text
    assert "[FINAL30][CONTRACT][FAIL][MA20]" in caplog.text
    assert "candidate_columns=" in caplog.text


def test_normalize_ma20_column_preserves_existing_values_and_scores() -> None:
    df = pd.DataFrame(_base_rows())
    original_ma20 = df["ma20"].copy()
    original_score_final = df["score_final"].copy()
    original_tech_score = df["tech_score"].copy()
    df["ma_20"] = [999.0 + idx for idx in range(len(df))]

    normalized = normalize_ma20_column(df, "test_preserve_existing")

    pd.testing.assert_series_equal(normalized["ma20"], original_ma20, check_names=False)
    pd.testing.assert_series_equal(normalized["score_final"], original_score_final, check_names=False)
    pd.testing.assert_series_equal(normalized["tech_score"], original_tech_score, check_names=False)