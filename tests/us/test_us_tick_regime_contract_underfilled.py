"""US tick entry gate must trust final30_trade_ready for underfilled contracts."""

from trader.us.runner.trade_tick_runner import validate_us_regime_contract_for_entry


def _contract(**overrides):
    base = {
        "contract_version": "us_sector_rotation_v3",
        "market_regime_version": "us_leading_regime_v1",
        "status": "OK_WITH_WARNINGS_CLUSTER_INCOMPLETE",
        "trade_can_proceed": 1,
        "trade_block_reason": "ok",
        "contract_ok": True,
        "final30_complete": False,
        "final30_trade_ready": True,
        "final30_scored_count": 25,
        "score_nonzero_count": 25,
        "cluster_contract_ok": True,
        "cap_violations": [],
        "underfilled_tier": "normal_underfilled",
        "effective_capital_scale": 0.7,
        "effective_max_new_positions": 15,
        "market_regime": "GROWTH_LEADERSHIP",
        "force_entry_block": False,
        "allow_new_buy": True,
    }
    base.update(overrides)
    return base


def _gate(contract):
    return validate_us_regime_contract_for_entry(
        contract,
        real_order_mode=False,
        kis_order_allowed=False,
    )


def test_final30_25_trade_ready_passes_even_when_complete_false():
    result = _gate(_contract())

    assert result["ok"] is True
    assert result["reason"] == "ok"


def test_final30_24_degraded_underfilled_trade_ready_passes():
    result = _gate(_contract(
        final30_scored_count=24,
        score_nonzero_count=24,
        underfilled_tier="degraded_underfilled",
        effective_capital_scale=0.5,
    ))

    assert result["ok"] is True
    assert result["reason"] == "ok"


def test_final30_14_not_trade_ready_blocks():
    result = _gate(_contract(
        status="FAILED_FINAL30_UNDERFILLED",
        trade_can_proceed=0,
        trade_block_reason="final30_below_absolute_min",
        contract_ok=False,
        final30_trade_ready=False,
        final30_scored_count=14,
        score_nonzero_count=14,
        underfilled_tier="blocked_underfilled",
    ))

    assert result["ok"] is False
    assert result["reason"] == "final30_below_absolute_min"


def test_risk_off_blocks_entry():
    result = _gate(_contract(
        status="RISK_OFF_ENTRY_BLOCKED",
        trade_can_proceed=1,
        market_regime="RISK_OFF",
        force_entry_block=False,
        allow_new_buy=True,
    ))

    assert result["ok"] is False
    assert result["reason"] == "risk_off_entry_block"


def test_score_mismatch_blocks():
    result = _gate(_contract(score_nonzero_count=24, final30_scored_count=25))

    assert result["ok"] is False
    assert result["reason"] == "score_contract_failed"


def test_cluster_cap_violation_blocks():
    result = _gate(_contract(cluster_contract_ok=False, cap_violations=["AI_TECH_COMBINED"]))

    assert result["ok"] is False
    assert result["reason"] == "cluster_cap_contract_failed"


def test_final30_complete_false_alone_does_not_block_when_trade_ready_true():
    result = _gate(_contract(final30_complete=False, final30_trade_ready=True))

    assert result["ok"] is True
    assert result["reason"] == "ok"


def test_crash_rebound_risk_off_contract_passes_but_sha_mismatch_stays_hard():
    rebound = _contract(
        status="OK_WITH_WARNINGS_CRASH_REBOUND_LIMITED",
        market_state="DEFENSE_CRASH_REBOUND",
        market_regime="RISK_OFF",
        entry_can_proceed=1,
        trade_block_reason="ok_crash_rebound_limited",
        effective_capital_scale=.25,
        effective_max_new_positions=3,
    )
    assert _gate(rebound)["ok"] is True
    mismatch = dict(rebound, contract_version="old")
    assert _gate(mismatch)["reason"] == "prep_contract_version_mismatch"
