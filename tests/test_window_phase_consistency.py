from __future__ import annotations

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
