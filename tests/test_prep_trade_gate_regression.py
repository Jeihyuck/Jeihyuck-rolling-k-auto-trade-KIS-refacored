from __future__ import annotations

import pandas as pd

from trader import prep_runner
from trader.runtime_paths import build_final30_scored_paths


def _row(idx: int, *, entry_style: str = "PULLBACK") -> dict:
    code = f"{idx + 1:06d}"
    return {
        "code": code,
        "name": f"N{code}",
        "rank_final30": idx + 1,
        "score_final": 90.0 + idx,
        "tech_score": 80.0 + idx,
        "flow_score": 10.0,
        "breakout_score": 20.0 + (idx % 5),
        "pullback_score": 30.0 + (idx % 7),
        "momentum_score": 40.0 + (idx % 9),
        "rs_percentile": 85.0,
        "vcp_score": 75.0,
        "entry_style_selected": entry_style,
        "ma20": 100.0 + idx,
        "ma50": 95.0 + idx,
        "ma150": 90.0 + idx,
        "close": 101.0 + idx,
        "atr_pct": 0.03,
        "pullback_pct": 0.02,
    }


def test_soft_fail_only_does_not_block_trade() -> None:
    result = prep_runner.decide_prep_trade_gate([], ["entry_style_monoculture"])

    assert result["status"] == "WARN"
    assert result["quality_ok"] == 1
    assert result["soft_fail"] == 1
    assert result["trade_can_proceed"] == 1


def test_sync_prep_final30_file_mirror_writes_all_targets(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(prep_runner, "repo_root", lambda: tmp_path)

    frame = pd.DataFrame([_row(idx) for idx in range(30)])
    result = prep_runner.sync_prep_final30_file_mirror(
        env="practice",
        as_of="2026-03-25",
        df=frame,
    )

    validation = result["validation"]
    paths = build_final30_scored_paths(tmp_path, "practice", "2026-03-25")

    assert validation["ok"] is True
    for label, path in paths.items():
        assert path.exists(), label
        assert int((validation["paths"].get(label) or {}).get("rows") or 0) == 30