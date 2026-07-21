import json
import os
import time

from trader.us.session_lock import acquire_session_lock


def test_us_am_running_lock_with_alive_pid_is_blocked(tmp_path):
    first = acquire_session_lock("us", "am", trade_date="2026-07-13", lock_dir=tmp_path)
    second = acquire_session_lock("us", "am", trade_date="2026-07-13", lock_dir=tmp_path)
    assert first.ok is True
    assert second.ok is False
    assert second.reason == "session_already_running"
    assert second.stale_pid == os.getpid()


def test_us_am_completed_recent_lock_is_blocked_by_interval(tmp_path):
    path = tmp_path / "us-am-2026-07-13.json"
    path.write_text(json.dumps({"status": "completed", "timestamp": time.time()}), encoding="utf-8")

    result = acquire_session_lock("us", "am", trade_date="2026-07-13", lock_dir=tmp_path)

    assert result.ok is False
    assert result.reason == "duplicate_recent_run"


def test_us_wsl_wrappers_use_shared_session_lock_helper():
    for path in ("scripts/wsl/run-us-am.sh", "scripts/wsl/run-us-afternoon.sh"):
        text = __import__("pathlib").Path(path).read_text(encoding="utf-8")
        assert "python" in text or "PYTHON_BIN" in text
        assert "-m trader.us.session_lock" in text
        assert "[US_SCHEDULER][DUPLICATE_BLOCKED]" in text
