from __future__ import annotations

import os

from trader.us.utils.session_guard import acquire_us_session_running_lock, release_us_session_running_lock


def test_us_session_running_lock_duplicate(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    first = acquire_us_session_running_lock("2026-06-22", "afternoon", "run-1")
    try:
        second = acquire_us_session_running_lock("2026-06-22", "afternoon", "run-2")
        assert first["acquired"] is True
        assert second["acquired"] is False
        assert second["reason"] == "SKIP_DUPLICATE_RUNNING"
    finally:
        release_us_session_running_lock("2026-06-22", "afternoon", run_id="run-1")


def test_us_session_running_lock_replaces_dead_pid(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "runtime/session_guard/us/2026-06-22/close.running"
    path.parent.mkdir(parents=True)
    path.write_text('{"pid": 99999999, "started_at_et": "2026-06-22T09:30:00-04:00"}', encoding="utf-8")
    got = acquire_us_session_running_lock("2026-06-22", "close", "run-new")
    try:
        assert got["acquired"] is True
    finally:
        release_us_session_running_lock("2026-06-22", "close", run_id="run-new")
