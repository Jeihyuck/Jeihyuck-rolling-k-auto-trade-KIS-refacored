from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.pb1_engine import PB1Engine


def test_entry_trigger_policy_marks_setup_override() -> None:
    policy = PB1Engine._resolve_entry_trigger_policy(
        trigger_ok=False,
        entry_ok=True,
        setup_filters_ok=True,
        decision_family="ENTRY_SETUP_OVERRIDE",
    )

    assert policy == "SETUP_OVERRIDE"


def test_explicit_trigger_bypass_blocks_none_policy(monkeypatch) -> None:
    monkeypatch.setenv("PB1_REQUIRE_EXPLICIT_TRIGGER_BYPASS", "1")

    entry_ok, reasons = PB1Engine._enforce_explicit_trigger_bypass(
        entry_ok=True,
        trigger_ok=False,
        trigger_policy="NONE",
        reasons=[],
    )

    assert entry_ok is False
    assert reasons == ["explicit_trigger_bypass_required"]


def test_entry_ohlcv_guard_blocks_insufficient_rows(monkeypatch) -> None:
    monkeypatch.setenv("PB1_BLOCK_INSUFFICIENT_OHLCV", "1")
    df = pd.DataFrame([{"date": f"2026-05-{idx:02d}", "close": 1} for idx in range(1, 60)])

    reason = PB1Engine._entry_ohlcv_block_reason(
        df=df,
        meta={"source": "db_short_only", "long_fetch_blocked": 1},
    )

    assert reason == "insufficient_ohlcv"


def test_entry_ohlcv_guard_allows_sufficient_history(monkeypatch) -> None:
    monkeypatch.setenv("PB1_BLOCK_INSUFFICIENT_OHLCV", "1")
    df = pd.DataFrame([{"date": f"2026-05-{idx:02d}", "close": 1} for idx in range(1, 130)])

    reason = PB1Engine._entry_ohlcv_block_reason(
        df=df,
        meta={"source": "db", "long_fetch_blocked": 0},
    )

    assert reason is None