from __future__ import annotations

from pathlib import Path

import pandas as pd

from trader.entry_engine.scanner import scan_entry_candidates
from trader.kis_wrapper import is_order_accepted
from trader.minervini_filter import normalize_vcp_score
from trader.path_contract import verify_final30_mirrors, write_final30_mirrors


def test_write_final30_mirrors_repairs_all_contract_paths(tmp_path: Path) -> None:
    rows = [
        {
            "code": f"{idx:06d}",
            "name": f"S{idx}",
            "score_final": 70 + idx,
            "tech_score": 60 + idx,
            "breakout_score": 50 + idx,
            "pullback_score": 40 + idx,
            "momentum_score": 30 + idx,
        }
        for idx in range(1, 31)
    ]

    results = write_final30_mirrors(
        repo_root=tmp_path,
        env="practice",
        as_of="2026-03-18",
        rows=rows,
        source="test",
    )

    assert set(results.keys()) == {"runtime", "ledger", "signals"}
    assert all(result["ok"] for result in results.values())

    verified = verify_final30_mirrors(
        repo_root=tmp_path,
        env="practice",
        as_of="2026-03-18",
        expected_rows=30,
    )
    assert all(result["ok"] for result in verified.values())


def test_scanner_precomputed_requires_real_context_not_score_presence() -> None:
    watchlist = [
        {"code": "000001", "name": "A", "score_final": 88, "tech_score": 80, "rs_percentile": 92, "vcp_score": 75, "entry_style_selected": "BREAKOUT"},
        {"code": "000002", "name": "B", "score_final": 77, "tech_score": 74, "rs_percentile": 85, "vcp_score": 68, "entry_style_selected": "MOMENTUM"},
    ]
    precomputed = pd.DataFrame(
        [
            {
                "code": "000001",
                "breakout_score": 95,
                "pullback_score": 90,
                "momentum_score": 91,
                "ma20": 0,
                "ma50": 0,
                "pullback_pct": 0,
                "rs_percentile": 92,
                "vcp_score": 75,
                "close": 100,
                "high_50": 0,
                "volume_avg20": 0,
                "volume": 0,
                "pivot_price": 0,
            },
            {
                "code": "000002",
                "breakout_score": 20,
                "pullback_score": 40,
                "momentum_score": 75,
                "ma20": 100,
                "ma50": 95,
                "pullback_pct": 0.06,
                "rs_percentile": 88,
                "vcp_score": 68,
                "close": 105,
                "high_50": 110,
                "volume_avg20": 1000,
                "volume": 1200,
                "pivot_price": 110,
            },
        ]
    )

    result = scan_entry_candidates(
        watchlist=watchlist,
        ohlcv_provider=lambda *_args, **_kwargs: None,
        precomputed_final30_df=precomputed,
        trade_precomputed_only=True,
    )

    all_codes = [signal.code for signal in result["all"]]
    assert all_codes == ["000002"]
    assert result["summary"]["setup_ok_count"] == 2
    assert not any(signal.code == "000001" for signal in result["breakout"])
    assert not any(signal.code == "000001" for signal in result["pullback"])
    assert any(signal.code == "000002" for signal in result["momentum"])


def test_normalize_vcp_score_rescales_low_range_inputs() -> None:
    assert normalize_vcp_score(2.67) == 26.7
    assert normalize_vcp_score(9.33) == 93.3


def test_is_order_accepted_handles_practice_response_without_order_number() -> None:
    assert is_order_accepted({"rt_cd": "0", "msg_cd": "MOCK_OK", "msg1": "practice accepted", "output": {}}, kis_env="practice")
    assert not is_order_accepted({"rt_cd": "1", "msg_cd": "REJECT", "msg1": "rejected", "output": {}}, kis_env="practice")