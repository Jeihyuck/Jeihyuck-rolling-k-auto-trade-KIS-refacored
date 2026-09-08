from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from trader.kr.pb1.entry_thresholds import is_intraday_threshold_window, resolve_entry_thresholds
from trader.pb1_engine import PB1Engine


def test_intraday_threshold_window_is_limited_to_entry_phases() -> None:
    now = datetime(2026, 6, 5, 14, 11, tzinfo=ZoneInfo("Asia/Seoul"))
    assert is_intraday_threshold_window(
        phase="entry",
        window_internal="morning",
        now_kst=now,
        entry_window_end="15:15",
    ) is True
    assert is_intraday_threshold_window(
        phase="close",
        window_internal="morning",
        now_kst=now,
        entry_window_end="15:15",
    ) is False


def test_resolve_entry_thresholds_respects_override_values() -> None:
    now = datetime(2026, 6, 5, 14, 11, tzinfo=ZoneInfo("Asia/Seoul"))
    intraday, thresholds = resolve_entry_thresholds(
        phase="entry",
        window_internal="morning",
        now_kst=now,
        entry_window_end="15:15",
        effective_entry_filters={
            "vol_max": 1.1,
            "volu_max": 2.2,
            "volu_max_intraday": 3.3,
            "pullback_min": 0.4,
            "pullback_max": 0.8,
            "require_both_contractions": True,
        },
        defaults={
            "vol_max": 1.0,
            "volu_max": 2.0,
            "volu_max_intraday": 3.0,
            "pullback_min": 0.5,
            "pullback_max": 0.9,
            "require_both_contractions": False,
        },
    )

    assert intraday is True
    assert thresholds == {
        "vol_contraction_max": 1.1,
        "volu_contraction_max": 3.3,
        "pullback_min": 0.4,
        "pullback_max": 0.8,
        "require_both_contractions": True,
    }


def test_pb1engine_resolve_filter_thresholds_matches_helper() -> None:
    now = datetime(2026, 6, 5, 14, 11, tzinfo=ZoneInfo("Asia/Seoul"))
    engine = PB1Engine.__new__(PB1Engine)
    engine.phase = "entry"
    engine.window_internal = "morning"
    engine._now_kst = now
    engine.effective_entry_filters = {
        "vol_max": 1.1,
        "volu_max": 2.2,
        "volu_max_intraday": 3.3,
        "pullback_min": 0.4,
        "pullback_max": 0.8,
        "require_both_contractions": True,
    }

    helper_intraday, helper_thresholds = resolve_entry_thresholds(
        phase=engine.phase,
        window_internal=engine.window_internal,
        now_kst=engine._now_kst,
        entry_window_end="15:15",
        effective_entry_filters=engine.effective_entry_filters,
        defaults={
            "vol_max": 1.0,
            "volu_max": 2.0,
            "volu_max_intraday": 3.0,
            "pullback_min": 0.5,
            "pullback_max": 0.9,
            "require_both_contractions": False,
        },
    )
    wrapper_thresholds = engine._resolve_filter_thresholds()

    assert helper_intraday is True
    assert helper_thresholds == {
        "vol_contraction_max": wrapper_thresholds.vol_contraction_max,
        "volu_contraction_max": wrapper_thresholds.volu_contraction_max,
        "pullback_min": wrapper_thresholds.pullback_min,
        "pullback_max": wrapper_thresholds.pullback_max,
        "require_both_contractions": wrapper_thresholds.require_both_contractions,
    }
