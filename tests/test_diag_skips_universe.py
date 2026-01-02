import os
from pathlib import Path

import pytest

from trader.pb1_engine import PB1Engine
from trader.window_router import WindowDecision


@pytest.fixture(autouse=True)
def _ensure_env(monkeypatch, tmp_path):
    monkeypatch.setenv("PB1_SKIP_UNIVERSE_IN_DIAG", "1")
    # Avoid git operations during tests
    monkeypatch.setattr("trader.pb1_engine.persist_run_files", lambda *args, **kwargs: None)
    # Keep ledger under a temp directory
    monkeypatch.setattr("trader.pb1_engine.LEDGER_BASE_DIR", Path(tmp_path / "ledger"))


def test_diagnostic_window_skips_universe(monkeypatch, tmp_path):
    window = WindowDecision(name="diagnostic", phase="verify")
    engine = PB1Engine(
        kis=None,
        worktree_dir=tmp_path,
        window=window,
        phase_override="auto",
        dry_run=True,
        env="paper",
        run_id="test-run",
        order_mode="dry_run",
        diag_level=1,
    )

    called = {"build": False}

    def _fail_build():
        called["build"] = True
        raise AssertionError("_build_universe should not run in diagnostic")

    monkeypatch.setattr(engine, "_build_universe", _fail_build)

    touched = engine.run()

    assert called["build"] is False
    assert any(p.name == "pnl_snapshot.json" and p.exists() for p in touched)
