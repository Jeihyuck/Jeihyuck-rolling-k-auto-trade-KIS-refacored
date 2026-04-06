from __future__ import annotations

import pytest
import pandas as pd

import trader.pb1_engine as pb1_engine
from trader.pb1_engine import PB1Engine, UniverseContext


def test_entry_guard_raises_system_exit_2_when_final30_missing(monkeypatch):
    engine = PB1Engine.__new__(PB1Engine)
    engine.env = "practice"
    engine.final30_locked = False
    engine.final30_df = pd.DataFrame()
    engine.final30_source = "none"
    engine._universe_context = None

    with pytest.raises(SystemExit) as exc:
        PB1Engine._load_entry_final30_or_abort(engine, "2026-02-20")

    assert exc.value.code == 2


def test_entry_guard_blocks_candidate_pool_members_as_final30_substitute() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    engine.env = "practice"
    engine.derived_as_of = "2026-02-20"
    engine.final30_locked = False
    engine.final30_df = pd.DataFrame()
    engine.final30_source = "candidate_pool_hit"
    engine._today = "2026-02-20"
    engine._universe_context = UniverseContext(
        as_of_date="2026-02-20",
        members=[{"code": f"{1000 + idx:06d}", "is_final30": False, "is_scored_final_input": False} for idx in range(30)],
        selected_path=None,
        meta={"source": "candidate_pool_hit"},
    )

    with pytest.raises(SystemExit) as exc:
        PB1Engine._load_entry_final30_or_abort(engine, "2026-02-20")

    assert exc.value.code == 2


def test_entry_guard_marks_stripped_members_contamination() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    engine.env = "practice"
    engine.derived_as_of = "2026-02-20"
    engine.final30_locked = True
    engine.final30_source = "db_pb1_watchlist_final_scored"
    engine.final30_df = pd.DataFrame([{"code": "005930"}])
    engine._today = "2026-02-20"
    engine._universe_context = UniverseContext(
        as_of_date="2026-02-20",
        members=[{"code": "005930"}],
        selected_path=None,
        meta={
            "source": "db_pb1_watchlist_final_scored",
            "locked_final30_rows": [{"code": f"{1000 + idx:06d}", "rank_final30": idx} for idx in range(30)],
        },
    )

    reason, _missing = PB1Engine._classify_locked_final30_abort_reason(engine, engine.final30_df)

    assert reason == "stripped_members_contamination"


def test_entry_guard_marks_locked_source_mismatch() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    engine.env = "practice"
    engine.derived_as_of = "2026-02-20"
    engine.final30_locked = True
    engine.final30_source = "watchlist_env"
    engine.final30_df = pd.DataFrame(
        [{"code": f"{1000 + idx:06d}", "rank_final30": idx} for idx in range(30)]
    )
    engine._today = "2026-02-20"
    engine._universe_context = UniverseContext(
        as_of_date="2026-02-20",
        members=[{"code": f"{1000 + idx:06d}"} for idx in range(30)],
        selected_path=None,
        meta={
            "source": "db_pb1_watchlist_final_scored",
            "locked_final30_rows": [{"code": f"{1000 + idx:06d}", "rank_final30": idx} for idx in range(30)],
        },
    )

    reason, _missing = PB1Engine._classify_locked_final30_abort_reason(engine, engine.final30_df)

    assert reason == "locked_final30_source_mismatch"
