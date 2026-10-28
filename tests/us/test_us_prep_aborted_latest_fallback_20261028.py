"""2026-10-27 interrupted re-PREP must never mask verified completed PREP."""
from __future__ import annotations

from trader.us import prep_contract


def _rows(run_id: str, n: int = 30) -> list[dict]:
    return [{"run_id": run_id, "symbol": f"US{i}", "score": float(i+1)}
            for i in range(n)]



def _entry_contract(*, entry: int = 1) -> dict:
    return {
        "entry_can_proceed": entry,
        "exit_can_proceed": 1,
        "trade_can_proceed": 1,
        "final30_trade_ready": True,
        "contract_ok": True,
        "allow_new_buy": bool(entry),
        "contract_version": "us_sector_rotation_v3",
        "market_regime_version": "us_leading_regime_v1",
        "trade_block_reason": "ok" if entry else "risk_off_entry_block",
        "force_entry_block": False,
    }


def _completed(run_id: str, status: str = "OK", *, entry: int = 1) -> dict:
    return {
        "status": status,
        "run_id": run_id,
        "finished_at": "2026-10-27T21:00:00Z",
        "result": _entry_contract(entry=entry),
    }

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
           completed=_completed("completed"),
           rows=_rows("completed"))
    result = prep_contract.check_us_prep_guard("2026-10-27")
    assert result["guard_state"] == "PREP_OK"
    assert result["source"] == "db_completed_fallback"
    assert result["run_id"] == "completed"
    assert result["entry_can_proceed"] is True


def test_interrupted_attempt_does_not_borrow_unrelated_locked_rows(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _setup(monkeypatch, latest={"status": "STARTED", "run_id": "interrupted"},
           completed=_completed("completed"),
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
           completed=_completed("finished", "OK_WITH_WARNINGS"),
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
                        lambda *a, **k: _completed("r1"))
    def unexpected(*a, **k):
        raise AssertionError("valid latest must not query the recovery fallback")
    monkeypatch.setattr(repos, "load_latest_completed_us_prep_status", unexpected)
    monkeypatch.setattr(repos, "load_locked_us_watchlist", lambda **k: _rows("r1"))
    assert prep_contract.check_us_prep_guard("2026-10-27")["source"] == "db"


def test_session_cache_ignores_started_if_exact_prior_completed_rows(monkeypatch):
    from trader.us.prep_effective import resolve_session_prep_cache
    latest = {"status": "STARTED", "run_id": "aborted"}
    completed = {"status": "OK", "run_id": "prior", "finished_at": "2026-10-27T21:00:00Z"}
    chosen, source = resolve_session_prep_cache(latest, completed, _rows("prior"))
    assert chosen["run_id"] == "prior"
    assert source == "completed_same_run_proven"


def test_session_cache_rejects_unproven_completed_run_and_missing_finish():
    from trader.us.prep_effective import resolve_session_prep_cache
    latest = {"status": "STARTED", "run_id": "aborted"}
    completed = {"status": "OK", "run_id": "prior", "finished_at": "2026-10-27T21:00:00Z"}
    chosen, source = resolve_session_prep_cache(latest, completed, _rows("other"))
    assert chosen["status"] == "STARTED"
    assert source == "unverified_incomplete"
    chosen, source = resolve_session_prep_cache(
        latest, {"status": "OK", "run_id": "prior"}, _rows("prior")
    )
    assert source == "unverified_incomplete"


def test_session_cache_does_not_pretend_latest_bad_prep_is_completed():
    from trader.us.prep_effective import resolve_session_prep_cache
    chosen, status = resolve_session_prep_cache(
        {"status": "ERROR", "run_id": "aborted"}, {}, _rows("aborted")
    )
    assert chosen["status"] == "ERROR"
    assert status == "unverified_incomplete"


def test_completed_defense_exit_only_remains_buy_blocked(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _setup(monkeypatch,
           latest={"status": "STARTED", "run_id": "aborted"},
           completed=_completed("defense", "DEFENSE_CRASH_ENTRY_BLOCKED", entry=0),
           rows=_rows("defense"))
    result = prep_contract.check_us_prep_guard("2026-10-27")
    assert result["entry_can_proceed"] is False
    assert result["exit_can_proceed"] is True


def test_completed_status_trade_only_does_not_authorize_buy(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    completed = _completed("prior")
    completed["result"].pop("entry_can_proceed")
    _setup(monkeypatch, latest={"status": "STARTED", "run_id": "aborted"},
           completed=completed, rows=_rows("prior"))
    result = prep_contract.check_us_prep_guard("2026-10-27")
    assert not result["entry_can_proceed"]
    assert "entry_can_proceed_not_proven" in result["reason"]


def test_latest_success_with_unrelated_run_rows_is_not_authoritative(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _setup(monkeypatch, latest=_completed("latest"),
           completed={}, rows=_rows("different"))
    result = prep_contract.check_us_prep_guard("2026-10-27")
    assert result["entry_can_proceed"] is False
    assert "watchlist_mismatch" in result["reason"]


def test_invalid_regime_version_never_promoted_to_buy(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    completed = _completed("prior")
    completed["result"]["market_regime_version"] = "invalid"
    _setup(monkeypatch, latest={"status": "STARTED", "run_id": "aborted"},
           completed=completed, rows=_rows("prior"))
    result = prep_contract.check_us_prep_guard("2026-10-27")
    assert not result["entry_can_proceed"]
    assert "version_unverified" in result["reason"]


def test_am_reprep_promotes_entry_only_with_fresh_guard_and_revision_pin():
    from trader.us.prep_effective import authorize_recovered_prep_guard
    prep = _completed("finished")
    prep["trade_date"] = "2026-10-27"
    prep["result"]["run_revision"] = "sha-pinned"
    guard = {
        "ok": True, "guard_state": "PREP_OK",
        "entry_can_proceed": True, "exit_can_proceed": True,
        "contract": {"run_id": "finished"},
    }
    kwargs = dict(candidate=prep, locked_rows=_rows("finished"),
                  fresh_guard=guard, trade_date="2026-10-27",
                  current_revision="sha-pinned",
                  manifest_revision_verified=True)
    allow, reason = authorize_recovered_prep_guard(**kwargs)
    assert allow is True
    assert reason == "recovered_prep_contract_and_guard_verified"
    for overrides in (
        {"manifest_revision_verified": False},
        {"current_revision": "wrong-sha"},
        {"locked_rows": _rows("interrupted")},
        {"fresh_guard": {**guard, "guard_state": "PREFLIGHT_EXIT_ONLY",
                         "entry_can_proceed": False}},
        {"fresh_guard": {**guard, "contract": {"run_id": "other"}}},
        {"fresh_guard": {**guard, "version_mismatch": True}},
    ):
        rejected = {**kwargs, **overrides}
        allow, reason = authorize_recovered_prep_guard(**rejected)
        assert allow is False, reason


def test_am_reprep_never_promotes_defensive_completed_exit_only():
    from trader.us.prep_effective import authorize_recovered_prep_guard
    prep = _completed("defense", "DEFENSE_CRASH_ENTRY_BLOCKED", entry=0)
    prep["result"]["run_revision"] = "sha-pinned"
    allow, reason = authorize_recovered_prep_guard(
        candidate=prep, locked_rows=_rows("defense"),
        fresh_guard={
            "ok": True, "entry_can_proceed": False,
            "exit_can_proceed": True, "guard_state": "PREP_DEGRADED_ENTRY_BLOCKED",
            "contract": {"run_id": "defense"},
        },
        trade_date="2026-10-27", current_revision="sha-pinned",
        manifest_revision_verified=True,
    )
    assert not allow
    assert reason == "prep_completion_not_entry_eligible"
