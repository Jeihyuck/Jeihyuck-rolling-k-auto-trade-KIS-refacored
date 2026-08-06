from __future__ import annotations


def test_revision_mismatch_allowed_when_hashes_compatible_and_latest_available():
    from trader.us.runner.trade_session_runner import _revision_mismatch_is_safe

    prep_contract = {
        "entry_can_proceed": 1,
        "final30_hash": "hash-final30",
        "selector_version": "bucket_champion_v2",
        "market_regime_version": "us_leading_regime_v1",
        "strategy_param_hash": "param-hash",
        "created_at": "2026-08-05T14:00:00+00:00",
    }
    latest_contract = {
        "entry_can_proceed": 1,
        "final30_hash": "hash-final30",
        "selector_version": "bucket_champion_v2",
        "market_regime_version": "us_leading_regime_v1",
        "strategy_param_hash": "param-hash",
        "created_at": "2026-08-05T14:05:00+00:00",
    }

    allowed, reason = _revision_mismatch_is_safe(prep_contract, latest_contract)
    assert allowed is True
    assert reason in {"final30_hash_equal", "latest_contract_available"}


def test_revision_mismatch_blocked_when_strategy_param_hash_differs():
    from trader.us.runner.trade_session_runner import _revision_mismatch_is_safe

    prep_contract = {
        "entry_can_proceed": 1,
        "final30_hash": "hash-final30",
        "selector_version": "bucket_champion_v2",
        "market_regime_version": "us_leading_regime_v1",
        "strategy_param_hash": "param-hash-a",
        "created_at": "2026-08-05T14:00:00+00:00",
    }
    latest_contract = {
        "entry_can_proceed": 1,
        "final30_hash": "hash-final30",
        "selector_version": "bucket_champion_v2",
        "market_regime_version": "us_leading_regime_v1",
        "strategy_param_hash": "param-hash-b",
        "created_at": "2026-08-05T14:05:00+00:00",
    }

    allowed, reason = _revision_mismatch_is_safe(prep_contract, latest_contract)
    assert allowed is False
    assert reason == "strategy_param_hash_mismatch"