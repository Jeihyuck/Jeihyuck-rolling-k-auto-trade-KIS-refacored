from __future__ import annotations

import pytest

import trader.pb1_engine as pb1_engine
from trader.pb1_engine import PB1Engine


def test_entry_guard_raises_system_exit_2_when_final30_missing(monkeypatch):
    engine = PB1Engine.__new__(PB1Engine)
    engine.env = "practice"

    monkeypatch.setattr(pb1_engine, "load_final30", lambda env, as_of: None)

    with pytest.raises(SystemExit) as exc:
        PB1Engine._load_entry_final30_or_abort(engine, "2026-02-20")

    assert exc.value.code == 2
