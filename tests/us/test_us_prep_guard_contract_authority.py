"""US prep guard must trust the prep contract trading authority fields."""

from trader.us import prep_contract as pc


def _guard(monkeypatch, contract):
    monkeypatch.setattr("trader.us.path_contract.load_us_prep_contract", lambda trade_date: {"trade_date": trade_date, **contract})
    return pc.check_us_prep_guard("2026-07-10")


def test_runtime_contract_final30_25_normal_underfilled_passes(monkeypatch):
    guard = _guard(monkeypatch, {
        "status": "OK_WITH_WARNINGS_CLUSTER_INCOMPLETE",
        "trade_can_proceed": 1,
        "trade_block_reason": "ok",
        "contract_ok": True,
        "final30_trade_ready": True,
        "final30_scored_count": 25,
        "score_nonzero_count": 25,
        "underfilled_tier": "normal_underfilled",
        "effective_capital_scale": 0.7,
        "effective_max_new_positions": 15,
    })
    assert guard["ok"] is True
    assert guard["trade_can_proceed"] is True
    assert guard["reason"] == "ok"
    assert guard["final30_scored_count"] == 25
    assert guard["underfilled_tier"] == "normal_underfilled"
    assert guard["effective_capital_scale"] == 0.7
    assert guard["effective_max_new_positions"] == 15


def test_runtime_contract_final30_24_degraded_underfilled_passes(monkeypatch):
    guard = _guard(monkeypatch, {
        "status": "OK_WITH_WARNINGS_CLUSTER_INCOMPLETE",
        "trade_can_proceed": 1,
        "trade_block_reason": "ok",
        "contract_ok": True,
        "final30_trade_ready": True,
        "final30_scored_count": 24,
        "score_nonzero_count": 24,
        "underfilled_tier": "degraded_underfilled",
    })
    assert guard["ok"] is True


def test_runtime_contract_final30_14_blocks_on_trade_can_proceed(monkeypatch):
    guard = _guard(monkeypatch, {
        "status": "FAILED_FINAL30_UNDERFILLED",
        "trade_can_proceed": 0,
        "trade_block_reason": "final30_below_absolute_min",
        "contract_ok": False,
        "final30_trade_ready": False,
        "final30_scored_count": 14,
        "score_nonzero_count": 14,
    })
    assert guard["ok"] is False
    assert "trade_can_proceed=0" in guard["reason"]


def test_runtime_contract_risk_off_blocks(monkeypatch):
    guard = _guard(monkeypatch, {
        "status": "RISK_OFF_ENTRY_BLOCKED",
        "trade_can_proceed": 0,
        "trade_block_reason": "risk_off_entry_block",
        "contract_ok": True,
        "final30_trade_ready": True,
        "final30_scored_count": 28,
        "score_nonzero_count": 28,
    })
    assert guard["ok"] is False
    assert "risk_off_entry_block" in guard["reason"]


def test_runtime_contract_score_mismatch_blocks_entry_only(monkeypatch):
    guard = _guard(monkeypatch, {
        "status": "OK_WITH_WARNINGS_CLUSTER_INCOMPLETE",
        "trade_can_proceed": 1,
        "trade_block_reason": "ok",
        "contract_ok": True,
        "final30_trade_ready": True,
        "final30_scored_count": 25,
        "score_nonzero_count": 24,
    })
    assert guard["ok"] is True
    assert guard["entry_can_proceed"] is False
    assert guard["exit_can_proceed"] is True
    assert "entry_blocked_by_final30_quality" in guard["reason"]


def test_db_fallback_trade_can_proceed_allows_cluster_incomplete(monkeypatch):
    monkeypatch.setattr("trader.us.path_contract.load_us_prep_contract", lambda trade_date: None)
    monkeypatch.setattr(
        "trader.us.db.repos.load_latest_us_prep_status",
        lambda trade_date, timeout_sec=20: {
            "status": "OK_WITH_WARNINGS_CLUSTER_INCOMPLETE",
            "trade_can_proceed": 1,
            "run_id": "run-1",
            "result": {
                "trade_can_proceed": 1,
                "trade_block_reason": "ok",
                "underfilled_tier": "normal_underfilled",
                "effective_capital_scale": 0.7,
                "effective_max_new_positions": 15,
            },
        },
    )
    monkeypatch.setattr(
        "trader.us.db.repos.load_locked_us_watchlist",
        lambda **kwargs: [{"symbol": f"T{i}", "score_final": 1.0} for i in range(25)],
    )
    guard = pc.check_us_prep_guard("2026-07-10")
    assert guard["ok"] is True
    assert guard["source"] == "db"
    assert guard["reason"] == "ok_db_fallback"
    assert guard["final30_scored_count"] == 25
