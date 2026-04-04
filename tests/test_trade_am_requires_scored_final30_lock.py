from __future__ import annotations

import logging

import pandas as pd
import pytest

from trader import pb1_runner


def test_trade_am_requires_scored_final30_lock(monkeypatch, caplog) -> None:
    monkeypatch.setenv("MODE", "trade")
    monkeypatch.setenv("TRADE_REQUIRE_PREP_FINAL30_SCORED", "1")
    monkeypatch.setenv("PB1_UNIVERSE_STRATEGY", "pb1_watchlist_final_scored")

    monkeypatch.setattr(
        pb1_runner,
        "load_locked_final30_from_db",
        lambda **_kwargs: {
            "df": pd.DataFrame(),
            "source_name": "db_pb1_watchlist_final_scored",
            "is_scored": False,
        },
    )

    caplog.set_level(logging.INFO)

    with pytest.raises(RuntimeError, match="final30_scored_lock_required"):
        pb1_runner._load_universe_context(
            engine=object(),
            as_of="2026-04-03",
            env="practice",
            strategy="pb1_watchlist_final_scored",
        )

    assert "[TRADE][FINAL30][LOCK][FAIL] reason=rows_not_30 rows=0" in caplog.text
    assert "[TRADE][ABORT] final30_scored_lock_required" in caplog.text