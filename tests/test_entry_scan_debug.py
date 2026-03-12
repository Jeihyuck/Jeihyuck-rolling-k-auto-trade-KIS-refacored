from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from trader.entry_engine.scanner import scan_entry_candidates


def _row(code: str) -> dict:
    return {
        "code": code,
        "name": f"N{code}",
        "score_final": 90.0,
        "tech_score": 80.0,
        "breakout_score": 70.0,
        "pullback_score": 60.0,
        "momentum_score": 50.0,
        "rs_percentile": 85.0,
        "vcp_score": 75.0,
        "entry_style_selected": "breakout",
    }


def test_entry_scan_writes_debug_when_zero_candidates(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ENTRY_SCAN_SAVE_DEBUG", "1")
    monkeypatch.setenv("ENTRY_SCAN_LOG_TOP_REJECTS", "10")
    monkeypatch.setenv("TRADE_FORCE_MIN1_DIAG", "1")

    def provider(_code: str, _days: int):
        return pd.DataFrame(
            {
                "date": ["2026-03-12"],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.0],
                "volume": [0.0],
            }
        )

    out = scan_entry_candidates(watchlist=[_row("005930"), _row("000660")], ohlcv_provider=provider)

    assert len(out["all"]) == 0

    trade_date = datetime.now().strftime("%Y-%m-%d")
    debug_path = Path("runtime") / "diagnostics" / trade_date / "entry_scan_debug.json"
    assert debug_path.exists()
    payload = json.loads(debug_path.read_text(encoding="utf-8"))
    assert "aggregate_rejected_counts" in payload
    assert "near_miss_candidates" in payload
