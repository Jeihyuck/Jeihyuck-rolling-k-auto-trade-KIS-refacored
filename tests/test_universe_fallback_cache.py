import json
from pathlib import Path

import pytest

from trader.pb1_engine import PB1Engine
from trader.window_router import WindowDecision


def test_universe_fallback_cache(monkeypatch, tmp_path):
    ledger_dir = tmp_path / "ledger"
    monkeypatch.setattr("trader.pb1_engine.LEDGER_BASE_DIR", ledger_dir)
    monkeypatch.setattr("trader.pb1_engine.persist_run_files", lambda *args, **kwargs: None)

    cache_path = tmp_path / "bot_state" / "trader_state" / "universe_cache.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cached_universe = {"selected_by_market": {"KOSPI": [{"code": "654321"}]}, "date": "2024-01-01", "source": "cache"}
    cache_path.write_text(json.dumps(cached_universe), encoding="utf-8")

    window = WindowDecision(name="diagnostic", phase="verify")

    engine = PB1Engine(
        kis=None,
        worktree_dir=tmp_path,
        window=window,
        phase_override="auto",
        dry_run=False,
        env="shadow",
        run_id="test-run",
        order_mode="shadow",
        diag_level=2,
    )

    monkeypatch.setattr("trader.pb1_engine.run_rebalance", lambda *args, **kwargs: {"selected_by_market": {}})

    selected = engine._build_universe()
    assert selected == cached_universe["selected_by_market"]
    assert "pykrx_universe_empty" in engine.diag_counters["fail_reasons"]
