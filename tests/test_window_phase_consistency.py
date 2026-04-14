from __future__ import annotations

import os

from trader.pb1_engine import PB1Engine
from trader.window_router import WindowDecision


class DummyOrdersRepo:
    def __init__(self):
        self.engine = object()


def test_morning_entry_window_phase_is_corrected():
    engine = PB1Engine(
        universe_repo=object(),
        orders_repo=DummyOrdersRepo(),
        fills_repo=object(),
        positions_repo=object(),
        ledger_repo=object(),
        kis=None,
        window=WindowDecision(name="morning", phase="exit"),
        window_label="morning",
        phase="entry",
        dry_run=True,
        env="practice",
        run_id="run-1",
        intended_live=False,
    )

    assert engine.window.phase == "entry"


def test_engine_init_accepts_explicit_phase_and_window_names():
    engine = PB1Engine(
        universe_repo=object(),
        orders_repo=DummyOrdersRepo(),
        fills_repo=object(),
        positions_repo=object(),
        ledger_repo=object(),
        kis=None,
        dry_run=True,
        env="practice",
        run_id="run-2",
        intended_live=False,
        phase_name="entry",
        window_name="intraday",
    )

    assert engine.phase_name == "entry"
    assert engine.window_name == "intraday"
    assert engine.window.name == "intraday"


def test_engine_init_defaults_without_phase_and_window():
    engine = PB1Engine(
        universe_repo=object(),
        orders_repo=DummyOrdersRepo(),
        fills_repo=object(),
        positions_repo=object(),
        ledger_repo=object(),
        kis=None,
        dry_run=True,
        env="practice",
        run_id="run-3",
        intended_live=False,
        phase_name=None,
        window_name=None,
    )

    assert engine.phase_name == "entry"
    assert engine.window_name == "day"
    assert engine.window.name == "day"


def test_engine_init_infers_phase_and_window_from_window_object():
    engine = PB1Engine(
        universe_repo=object(),
        orders_repo=DummyOrdersRepo(),
        fills_repo=object(),
        positions_repo=object(),
        ledger_repo=object(),
        kis=None,
        window=WindowDecision(name="intraday", phase="entry"),
        dry_run=True,
        env="practice",
        run_id="run-4",
        intended_live=False,
    )

    assert engine.phase_name == "entry"
    assert engine.window_name == "intraday"
    assert engine.window.phase == "entry"


def test_engine_init_normalizes_am_window_to_morning():
    previous = os.environ.get("PB1_SESSION_KIND")
    os.environ["PB1_SESSION_KIND"] = "am"
    try:
        engine = PB1Engine(
            universe_repo=object(),
            orders_repo=DummyOrdersRepo(),
            fills_repo=object(),
            positions_repo=object(),
            ledger_repo=object(),
            kis=None,
            dry_run=True,
            env="practice",
            run_id="run-5",
            intended_live=False,
            phase_name="entry",
            window_name="intraday",
        )
    finally:
        if previous is None:
            os.environ.pop("PB1_SESSION_KIND", None)
        else:
            os.environ["PB1_SESSION_KIND"] = previous

    assert engine.window_name == "morning"
    assert engine.window.name == "morning"
