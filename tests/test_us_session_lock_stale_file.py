import json

from trader.us.session_lock import acquire_session_lock


def test_stale_scheduler_lock_without_pid_is_removed(tmp_path):
    path = tmp_path / "us-am-2026-07-21.json"
    path.write_text(json.dumps({
        "market": "us", "session": "am", "trade_date": "2026-07-21", "status": "running",
    }), encoding="utf-8")

    result = acquire_session_lock("us", "am", trade_date="2026-07-21", lock_dir=tmp_path)

    assert result.ok is True
    assert result.reason == "stale_lock_removed"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["pid"] > 0
    assert payload["created_at"]
    assert payload["hostname"]
    assert payload["cmdline"]
