"""AMGN raw account versus hashed snapshot regression without broker I/O."""
from __future__ import annotations

import hashlib
from unittest.mock import patch

from trader.us.account_scope_identity import verify_us_account_scope
from trader.us.stop_policy_revision import scope_for_position


RAW = "practice-original-broker-account"
DIGEST = hashlib.sha256(RAW.encode()).hexdigest()


def _position(account=DIGEST, *, meta_account=RAW, claimed_hash=DIGEST):
    return {
        "symbol": "AMGN", "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "US:AMGN:original-cycle",
        "account_id": account,
        "broker_account_hash": claimed_hash,
        "meta": {
            "account_id": meta_account,
            "broker_account_hash": claimed_hash,
            "strategy_owner": "US_STANDARD",
        },
        "trading_epoch_id": "epoch-one",
        "portfolio_epoch_id": "portfolio-one",
        "position_cycle_id": "US:AMGN:original-cycle",
    }


def test_same_account_raw_and_hash_are_exactly_normalized_in_us_wsl():
    with patch.dict("os.environ", {"KIS_ENV": "practice",
                                   "WSL_RUN_MARKET": "US"}):
        with patch("trader.account_state.get_account_key", return_value=RAW):
            assert scope_for_position(_position()) == (
                "AMGN", "US:AMGN:original-cycle", RAW
            )
            assert scope_for_position(_position(account=RAW)) == (
                "AMGN", "US:AMGN:original-cycle", RAW
            )


def test_real_conflicts_and_double_hash_fail_closed():
    wrong = hashlib.sha256(DIGEST.encode()).hexdigest()
    for pos in [
        _position(account=wrong),
        _position(meta_account="someone-else"),
        _position(claimed_hash=wrong),
        _position(account=""),
    ]:
        with patch.dict("os.environ", {"KIS_ENV": "practice",
                                       "WSL_RUN_MARKET": "US"}):
            with patch("trader.account_state.get_account_key", return_value=RAW):
                assert scope_for_position(pos) == ("", "", "")
    assert verify_us_account_scope(
        trusted_raw_account=RAW, claims=[DIGEST, "other"]
    ) is None


def test_no_account_type_guessing_or_side_effect_to_tqqq_owner():
    assert verify_us_account_scope(
        trusted_raw_account=RAW, claims=[RAW, DIGEST],
        declared_hashes=[DIGEST],
    ).raw_account == RAW
    assert verify_us_account_scope(
        trusted_raw_account=RAW, claims=[DIGEST], declared_hashes=[RAW],
    ) is None
    tqqq = _position()
    tqqq["symbol"] = "TQQQ"
    tqqq["strategy_owner"] = "TQQQ_INFINITE"
    with patch.dict("os.environ", {"KIS_ENV": "practice",
                                   "WSL_RUN_MARKET": "US"}):
        with patch("trader.account_state.get_account_key", return_value=RAW):
            assert scope_for_position(tqqq) == ("", "", "")


def test_non_us_runtime_preserves_historical_raw_scope():
    with patch.dict("os.environ", {"WSL_RUN_MARKET": ""}):
        assert scope_for_position(_position(account=RAW)) == (
            "AMGN", "US:AMGN:original-cycle", RAW
        )
