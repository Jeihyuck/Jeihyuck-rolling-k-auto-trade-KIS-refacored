from __future__ import annotations

import pandas as pd
import pytest

from trader import pb1_runner
from trader.db.repos import ScoredWatchlistInvalidError, ScoredWatchlistNotFoundError


def _rows() -> list[dict]:
    return [
        {
            "as_of": "2026-04-02",
            "code": f"{9000 + idx:06d}",
            "rank_final30": idx,
            "score_final": 100.0 + idx,
            "tech_score": 80.0 + idx,
            "breakout_score": 70.0 + idx,
            "pullback_score": 60.0 + idx,
            "momentum_score": 50.0 + idx,
            "rs_percentile": 95.0,
            "vcp_score": 75.0,
            "entry_style_selected": "BREAKOUT",
            "ma20": 100.0 + idx,
            "ma50": 90.0 + idx,
            "ma150": 80.0 + idx,
            "atr_pct": 0.03,
            "close": 110.0 + idx,
            "reasons": ["db_only"],
            "filters_passed": ["scored"],
            "filters_failed": [],
        }
        for idx in range(1, 31)
    ]


def test_load_locked_final30_from_db_succeeds_with_db_only_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        pb1_runner,
        "load_final30_scored_exact",
        lambda *_args, **_kwargs: pd.DataFrame(_rows()),
    )

    result = pb1_runner.load_locked_final30_from_db(
        engine=object(),
        env="practice",
        derived_as_of="2026-04-02",
    )

    assert result["source_name"] == "db_pb1_watchlist_final_scored"
    assert result["locked"] is True
    assert result["rows"] == 30
    assert len(result["codes"]) == 30


def test_load_locked_final30_from_db_aborts_when_scored_rows_missing(monkeypatch) -> None:
    def _raise_missing(*_args, **_kwargs):
        raise ScoredWatchlistNotFoundError(
            "scored_final30_missing",
            expected_strategy="pb1_watchlist_final_scored",
            actual_strategy="none",
            rows=0,
            missing_cols=["score_final"],
        )

    monkeypatch.setattr(pb1_runner, "load_final30_scored_exact", _raise_missing)

    with pytest.raises(RuntimeError, match="ENTRY_ABORT_PRECHECK:db_exact_scored_zero_rows"):
        pb1_runner.load_locked_final30_from_db(
            engine=object(),
            env="practice",
            derived_as_of="2026-04-02",
        )


def test_hydrate_locked_final30_from_db_only_rejects_invalid_contract(monkeypatch) -> None:
    def _raise_invalid(*_args, **_kwargs):
        raise ScoredWatchlistInvalidError(
            "required_scored_cols_missing",
            expected_strategy="pb1_watchlist_final_scored",
            actual_strategy="pb1_watchlist_final_scored",
            rows=30,
            missing_cols=["entry_style_selected"],
        )

    monkeypatch.setattr(pb1_runner, "load_final30_scored_exact", _raise_invalid)

    with pytest.raises(RuntimeError, match="ENTRY_ABORT_PRECHECK:db_exact_scored_missing_critical_cols"):
        pb1_runner._hydrate_locked_final30_from_db_only(
            engine=object(),
            env="practice",
            as_of="2026-04-02",
        )


def test_assert_engine_boot_locked_final30_accepts_db_only_dataframe() -> None:
    run_ctx = {
        "final30_source": "db_pb1_watchlist_final_scored",
        "final30_locked": True,
    }

    pb1_runner._assert_engine_boot_locked_final30(
        run_ctx=run_ctx,
        final30_df=pd.DataFrame(_rows()),
    )


def test_assert_engine_boot_locked_final30_rejects_missing_dataframe() -> None:
    run_ctx = {
        "final30_source": "db_pb1_watchlist_final_scored",
        "final30_locked": True,
    }

    with pytest.raises(RuntimeError, match="ENTRY_ABORT_PRECHECK:db_exact_scored_zero_rows"):
        pb1_runner._assert_engine_boot_locked_final30(
            run_ctx=run_ctx,
            final30_df=pd.DataFrame(),
        )


def test_hydrate_locked_final30_from_db_only_succeeds_without_any_files(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(pb1_runner, "resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pb1_runner, "load_final30_scored_exact", lambda *_args, **_kwargs: pd.DataFrame(_rows()))

    df = pb1_runner._hydrate_locked_final30_from_db_only(
        engine=object(),
        env="practice",
        as_of="2026-04-02",
    )

    assert len(df) == 30


def test_hydrate_locked_final30_from_db_only_fails_even_if_file_exists(tmp_path, monkeypatch) -> None:
    runtime_dir = tmp_path / "runtime" / "watchlist" / "2026-04-02"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "final30_scored.json").write_text("[]", encoding="utf-8")
    monkeypatch.setattr(pb1_runner, "resolve_repo_root", lambda: tmp_path)

    def _raise_missing(*_args, **_kwargs):
        raise ScoredWatchlistNotFoundError(
            "scored_final30_missing",
            expected_strategy="pb1_watchlist_final_scored",
            actual_strategy="none",
            rows=0,
            missing_cols=["score_final"],
        )

    monkeypatch.setattr(pb1_runner, "load_final30_scored_exact", _raise_missing)

    with pytest.raises(RuntimeError, match="ENTRY_ABORT_PRECHECK:db_exact_scored_zero_rows"):
        pb1_runner._hydrate_locked_final30_from_db_only(
            engine=object(),
            env="practice",
            as_of="2026-04-02",
        )