"""2026-10-27 interrupted re-PREP must never mask verified completed PREP."""
from __future__ import annotations

from trader.us import prep_contract


def _rows(run_id: str, n: int = 30) -> list[dict]:
    return [{"run_id": run_id, "symbol": f"US{i}", "score": float(i+1)}
            for i in range(n)]


def _setup(monkeypatch, *, latest, completed, rows):
    import trader.us.db.repos as repos
    monkeypatch.setattr(
        "trader.us.path_contract.load_us_prep_contract",
        lambda *_: None,
    )
    monkeypatch.setattr(repos, "load_latest_us_prep_status",
                        lambda *a, **k: latest)
    monkeypatch.setattr(repos, "load_latest_completed_us_prep_status",
                        lambda *a, **k: completed)
    monkeypatch.setattr(repos, "load_locked_us_watchlist",
                        lambda **k: rows)


def test_started_attempt_does_not_mask_completed_prep_with_matching_rows(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _setup(monkeypatch, latest={"status": "STARTED", "run_id": "interrupted"},
           completed={"status": "OK", "run_id": "completed"},
           rows=_rows("completed"))
    result = prep_contract.check_us_prep_guard("2026-10-27")
    assert result["guard_state"] == "PREP_OK"
    assert result["source"] == "db_completed_fallback"
    assert result["run_id"] == "completed"
    assert result["entry_can_proceed"] is True


def test_interrupted_attempt_does_not_borrow_unrelated_locked_rows(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _setup(monkeypatch, latest={"status": "STARTED", "run_id": "interrupted"},
           completed={"status": "OK", "run_id": "completed"},
           rows=_rows("interrupted"))
    result = prep_contract.check_us_prep_guard("2026-10-27")
    assert result["entry_can_proceed"] is False
    assert result["guard_state"] == "PREP_DEGRADED_ENTRY_BLOCKED"
    assert "completed_run_watchlist_mismatch" in result["reason"]


def test_no_completed_prep_remains_exit_only(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _setup(monkeypatch, latest={"status": "STARTED", "run_id": "interrupted"},
           completed={}, rows=_rows("interrupted"))
    result = prep_contract.check_us_prep_guard("2026-10-27")
    assert result["entry_can_proceed"] is False
    assert result["exit_can_proceed"] is True
    assert "STARTED" in result["reason"]


def test_error_attempt_does_not_mask_completed_prep(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _setup(monkeypatch, latest={"status": "ERROR", "run_id": "interrupted"},
           completed={"status": "OK_WITH_WARNINGS", "run_id": "finished"},
           rows=_rows("finished"))
    result = prep_contract.check_us_prep_guard("2026-10-27")
    assert result["entry_can_proceed"] is True
    assert result["run_id"] == "finished"


def test_existing_latest_valid_prep_does_not_query_fallback(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    import trader.us.db.repos as repos
    monkeypatch.setattr("trader.us.path_contract.load_us_prep_contract",
                        lambda *_: None)
    monkeypatch.setattr(repos, "load_latest_us_prep_status",
                        lambda *a, **k: {"status": "OK", "run_id": "r1"})
    def unexpected(*a, **k):
        raise AssertionError("valid latest must not query the recovery fallback")
    monkeypatch.setattr(repos, "load_latest_completed_us_prep_status", unexpected)
    monkeypatch.setattr(repos, "load_locked_us_watchlist", lambda **k: _rows("r1"))
    assert prep_contract.check_us_prep_guard("2026-10-27")["source"] == "db"
