from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from trader.pb1_engine import resolve_pb1_phase
from trader.strategies.pb1_pullback_close import evaluate_setup


def test_afternoon_late_start_resolves_pm_entry_before_close(monkeypatch):
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "afternoon")
    monkeypatch.setenv("PB1_PM_SESSION_END", "15:10")
    phase, reason, _window = resolve_pb1_phase(
        datetime(2026, 6, 9, 13, 41, tzinfo=ZoneInfo("Asia/Seoul")),
        True,
        "exit",
    )
    assert phase == "pm_entry"
    assert reason == "afternoon_entry_allowed"


def test_afternoon_close_window_resolves_close(monkeypatch):
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "afternoon")
    monkeypatch.setenv("PB1_PM_SESSION_END", "15:10")
    phase, reason, _window = resolve_pb1_phase(
        datetime(2026, 6, 9, 15, 12, tzinfo=ZoneInfo("Asia/Seoul")),
        True,
        "pm_entry",
    )
    assert phase == "close"
    assert reason == "close_window"


def test_evaluate_setup_uses_explicit_pb1_threshold_overrides():
    ok, reasons = evaluate_setup(
        {
            "close": 101.0,
            "ma20": 100.0,
            "ma50": 90.0,
            "pullback_pct": 5.0,
            "vol_contraction": 1.20,
            "volu_contraction": 1.20,
            "ma20_slope": 1.0,
            "volume_missing": False,
            "_pb1_vol_max": 1.25,
            "_pb1_volu_max": 1.25,
        },
        "KOSPI",
        require_volume=True,
        mode="relaxed",
        relax_ma_filter=True,
        relax_ma20_slope=True,
    )
    assert ok, reasons
    assert "vol_contraction_fail" not in reasons
    assert "volu_contraction_fail" not in reasons


def test_intraday_reclaim_equivalent_features_pass_close_below_ma20():
    last_close = 99.0
    ma20 = 100.0
    current_price = ma20 * 1.001
    features = {
        "last_close": last_close,
        "close": current_price,
        "current_price": current_price,
        "ma20": ma20,
        "ma50": 90.0,
        "pullback_pct": 5.0,
        "vol_contraction": 0.9,
        "volu_contraction": 0.9,
        "ma20_slope": 1.0,
        "volume_missing": False,
        "quality_flags": ["INTRADAY_RECLAIM_MA20"],
    }
    ok, reasons = evaluate_setup(
        features,
        "KOSPI",
        require_volume=True,
        mode="relaxed",
        relax_ma_filter=True,
        relax_ma20_slope=True,
    )
    assert ok, reasons
    assert "close_below_ma20" not in reasons
    assert "INTRADAY_RECLAIM_MA20" in features["quality_flags"]
