from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from trader.pb1_engine import PB1Engine
from trader.kr.pb1.entry_cutoff import resolve_entry_cutoff


def test_resolve_entry_cutoff_uses_env_override() -> None:
    now_kst = datetime(2026, 6, 5, 14, 11, tzinfo=ZoneInfo("Asia/Seoul"))

    cutoff, raw = resolve_entry_cutoff(
        now_kst=now_kst,
        entry_window_end="15:15",
        entry_cutoff_time="15:10",
    )

    assert cutoff.isoformat() == "2026-06-05T15:10:00+09:00"
    assert raw == "15:10"


def test_resolve_entry_cutoff_falls_back_to_window_end() -> None:
    now_kst = datetime(2026, 6, 5, 14, 11, tzinfo=ZoneInfo("Asia/Seoul"))

    cutoff, raw = resolve_entry_cutoff(
        now_kst=now_kst,
        entry_window_end="15:15",
        entry_cutoff_time="bad-value",
    )

    assert cutoff.isoformat() == "2026-06-05T15:15:00+09:00"
    assert raw == "15:15"


def test_pb1engine_resolve_entry_cutoff_matches_helper(monkeypatch) -> None:
    monkeypatch.setenv("ENTRY_CUTOFF_TIME", "15:10")
    now_kst = datetime(2026, 6, 5, 14, 11, tzinfo=ZoneInfo("Asia/Seoul"))
    engine = PB1Engine.__new__(PB1Engine)
    engine._now_kst = now_kst

    helper_result = resolve_entry_cutoff(
        now_kst=now_kst,
        entry_window_end="15:15",
        entry_cutoff_time="15:10",
    )
    wrapper_result = engine._resolve_entry_cutoff()

    assert helper_result == wrapper_result
