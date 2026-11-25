"""Regression for the 2026-11-23 US verified snapshot read boundary."""
import hashlib

import pytest

from trader.us.position_lifecycle_state import _identity_is_current, _position_identity


RAW = "practice:12345678:01"
HASH = hashlib.sha256(RAW.encode("utf-8")).hexdigest()
EPOCH = "test-epoch"


def snapshot():
    return {
        "symbol": "MDT",
        "account_id": HASH,
        "trading_epoch_id": EPOCH,
        "portfolio_epoch_id": "portfolio",
        "position_cycle_id": "cycle",
        "meta": {
            "env": "practice",
            "account_id": RAW,
            "broker_account_hash": HASH,
            "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": "cycle",
        },
    }


def test_db_snapshot_and_broker_view_resolve_same_lifecycle_account(monkeypatch):
    import trader.account_state as account_state
    import trader.us.db.repos as repos
    monkeypatch.setattr(account_state, "resolve_env_name", lambda: "practice")
    monkeypatch.setattr(account_state, "get_account_key", lambda **kw: RAW)
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: object())
    monkeypatch.setattr(repos, "_active_us_epoch", lambda: EPOCH)

    stored = snapshot()
    broker_view = {**stored, "account_id": RAW}
    db_identity = _position_identity(stored, env="practice")
    assert db_identity == _position_identity(broker_view, env="practice")
    assert db_identity["account_id"] == RAW
    assert db_identity["position_cycle_id"] == "cycle"
    assert db_identity["portfolio_epoch_id"] == "portfolio"
    assert _identity_is_current(db_identity)
    assert stored["account_id"] == HASH  # do not rewrite durable scope


@pytest.mark.parametrize("mutation", ["other_raw", "other_hash", "missing_hash", "conflicting_hash"])
def test_conflicting_account_proof_is_not_adopted(monkeypatch, mutation):
    import trader.us.db.repos as repos
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    row = snapshot()
    if mutation == "other_raw":
        row["meta"]["account_id"] = "practice:87654321:01"
    elif mutation == "other_hash":
        row["account_id"] = "f" * 64
    elif mutation == "missing_hash":
        del row["meta"]["broker_account_hash"]
    else:
        row["broker_account_hash"] = "e" * 64
    identity = _position_identity(row, env="practice")
    assert identity["account_id"] != RAW
    assert not _identity_is_current(identity)
