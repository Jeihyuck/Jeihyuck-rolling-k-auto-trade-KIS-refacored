from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from trader.candidate_pool_builder import CandidatePoolBuilder


def _make_members(count: int) -> list[dict]:
    return [{"code": f"{i + 1:06d}", "name": f"Stock{i + 1}"} for i in range(count)]


def _make_ohlcv_df(rank_idx: int, total: int, vcp_score: float, rows: int = 220) -> pd.DataFrame:
    # Higher rank gets stronger return profile, which increases cross-sectional RS percentile.
    growth = 1.0 + ((total - rank_idx) / float(total)) * 0.6
    close = np.linspace(100.0, 100.0 * growth, rows)

    prior_volume = 1_000.0
    recent_volume = prior_volume * max(0.0, 1.0 - (vcp_score / 100.0))
    volume = np.full(rows, prior_volume, dtype=float)
    volume[-10:] = recent_volume

    return pd.DataFrame({"close": close, "volume": volume})


def _build_provider(count: int, vcp_by_index: dict[int, float]):
    data_map = {}
    for idx, member in enumerate(_make_members(count)):
        vcp = float(vcp_by_index.get(idx, 20.0))
        data_map[member["code"]] = _make_ohlcv_df(rank_idx=idx, total=count, vcp_score=vcp)

    def _provider(code: str, days: int = 120):
        return data_map[code].tail(days).copy()

    return _provider


def _set_threshold_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RS_MIN_PCTILE", "70")
    monkeypatch.setenv("VCP_MIN_SCORE", "60")
    monkeypatch.setenv("PB1_BOOTSTRAP_ENABLE", "1")
    monkeypatch.setenv("BOOTSTRAP_MINERVINI_RS_MIN_PCTILE", "60")
    monkeypatch.setenv("BOOTSTRAP_MINERVINI_VCP_MIN_SCORE", "45")
    monkeypatch.setenv("BOOTSTRAP_RELAX_PASSES", "5")
    monkeypatch.setenv("CANDIDATE_POOL_MIN_SIZE", "40")


def test_strict_fail_but_relax_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_threshold_env(monkeypatch)
    count = 165
    members = _make_members(count)

    # strict(70/60) keeps ~1, relaxed floor(60/45) keeps >= 40
    vcp_map = {0: 65.0}
    for i in range(1, 40):
        vcp_map[i] = 50.0

    builder = CandidatePoolBuilder(
        ohlcv_provider=_build_provider(count, vcp_map),
        target_size=120,
        min_price=1.0,
        min_rows=30,
    )

    selected = builder.build_light_scan(members=members, as_of=date(2026, 3, 13))
    report = builder.last_build_report

    assert len(selected) >= 40
    assert int(report.get("strict_kept", 0)) <= 3
    assert int(report.get("relaxed_kept", 0)) >= 40
    assert int(report.get("selection_mode_counts", {}).get("relaxed_minervini", 0)) > 0


def test_strict_relax_shortage_fallback_topup(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_threshold_env(monkeypatch)
    count = 165
    members = _make_members(count)

    # relaxed floor keeps 18 only -> fallback must topup to 40
    vcp_map = {0: 65.0}
    for i in range(1, 18):
        vcp_map[i] = 50.0

    builder = CandidatePoolBuilder(
        ohlcv_provider=_build_provider(count, vcp_map),
        target_size=120,
        min_price=1.0,
        min_rows=30,
    )

    selected = builder.build_light_scan(members=members, as_of=date(2026, 3, 13))
    report = builder.last_build_report

    assert len(selected) == 40
    assert int(report.get("relaxed_kept", 0)) == 18
    assert int(report.get("selection_mode_counts", {}).get("fallback_topup", 0)) > 0


def test_bootstrap_thresholds_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_threshold_env(monkeypatch)
    count = 165
    members = _make_members(count)

    builder = CandidatePoolBuilder(
        ohlcv_provider=_build_provider(count, {0: 65.0, 1: 50.0}),
        target_size=120,
        min_price=1.0,
        min_rows=30,
    )

    thresholds = builder.resolve_minervini_thresholds()
    builder.build_light_scan(members=members, as_of=date(2026, 3, 13))
    report = builder.last_build_report

    assert thresholds["bootstrap_rs_min"] == 60.0
    assert thresholds["bootstrap_vcp_min"] == 45.0
    assert thresholds["relax_passes"] == 5
    assert report["thresholds_used"]["floor_rs_min"] == 60.0
    assert report["thresholds_used"]["floor_vcp_min"] == 45.0


def test_only_data_shortage_hard_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_threshold_env(monkeypatch)
    members = _make_members(5)

    builder = CandidatePoolBuilder(
        ohlcv_provider=_build_provider(5, {0: 65.0, 1: 50.0, 2: 50.0}),
        target_size=120,
        min_price=1.0,
        min_rows=30,
    )

    with pytest.raises(RuntimeError) as exc_info:
        builder.build_light_scan(members=members, as_of=date(2026, 3, 13))

    message = str(exc_info.value)
    assert "prefilter_survivors=" in message
    assert "strict_kept=" in message
    assert "fallback_eligible=" in message
