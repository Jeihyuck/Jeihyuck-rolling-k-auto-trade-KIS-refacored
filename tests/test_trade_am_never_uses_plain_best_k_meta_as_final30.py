from __future__ import annotations

import logging

import pandas as pd
import pytest

from trader import pb1_runner


def test_trade_am_never_uses_plain_best_k_meta_as_final30(monkeypatch, caplog) -> None:
    monkeypatch.setenv("MODE", "trade")
    monkeypatch.setenv("TRADE_REQUIRE_PREP_FINAL30_SCORED", "1")
    monkeypatch.setenv("PB1_UNIVERSE_STRATEGY", "pb1_watchlist_final_scored")

    plain_rows = pd.DataFrame(
        {
            "as_of_date": ["2026-04-03"] * 196,
            "code": [f"{100000 + idx:06d}" for idx in range(196)],
            "env": ["practice"] * 196,
            "market": ["KOSPI"] * 196,
            "market_cap": [1000] * 196,
            "name": ["TEST"] * 196,
            "provider": ["db"] * 196,
            "rank": list(range(196)),
            "reason": ["plain"] * 196,
            "strategy": ["best_k_meta"] * 196,
        }
    )
    monkeypatch.setattr(
        pb1_runner,
        "load_locked_final30_from_db",
        lambda **_kwargs: {
            "df": plain_rows,
            "source_name": "best_k_meta",
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

    assert "[TRADE][FINAL30][LOAD_RESULT] source=best_k_meta rows=196 is_scored=0" in caplog.text
    assert "[TRADE][FINAL30][LOCK][FAIL] reason=source_not_scored actual_source=best_k_meta" in caplog.text