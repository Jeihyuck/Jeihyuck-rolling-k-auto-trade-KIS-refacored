"""Existing PB1 stops move to 3/6 only via a scoped, verified DB revision."""
from trader.us.entry_exit_contract import (
    build_us_entry_exit_contract, contract_exit_config, verify_us_entry_exit_contract,
)
from trader.us.stop_policy_revision import (
    REVISION_CODE, validated_revision, attach_approved_revisions,
)


def _legacy(monkeypatch):
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")
    monkeypatch.setenv("US_SOFT_STOP_LOSS_PCT", "0.05")
    contract = build_us_entry_exit_contract({"symbol": "AMD", "strategy_owner": "US_STANDARD"})
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.06")
    monkeypatch.setenv("US_SOFT_STOP_LOSS_PCT", "0.03")
    return contract


def _position(contract):
    return {
        "symbol":"AMD", "strategy_owner":"US_STANDARD", "account_id":"practice-account",
        "position_lifecycle_id":"amd-life", "qty":4, "entry_price":100.0,
        "meta":{"entry_exit_contract":contract},
    }


def _revision(contract, **changes):
    payload = {
        "symbol":"AMD", "lifecycle_id":"amd-life", "account_id":"practice-account",
        "strategy_owner":"US_STANDARD", "source_contract_sha256":contract["sha256"],
        "revision_code":REVISION_CODE, "soft_stop":0.03, "hard_stop":0.06,
        "_db_verified":True,
    }
    payload.update(changes)
    return payload


def test_explicit_approved_revision_only_overrides_stops(monkeypatch):
    contract = _legacy(monkeypatch)
    before_sha = contract["sha256"]
    position = _position(contract)
    assert contract_exit_config(position)["soft_stop"] == 0.05
    assert contract_exit_config(position)["hard_stop"] == 0.08
    position["_verified_pb1_stop_revision"] = _revision(contract)
    after = contract_exit_config(position)
    assert (after["soft_stop"], after["hard_stop"]) == (0.03,0.06)
    assert after["giveback"] == contract["management"]["swing"]["giveback"]
    assert verify_us_entry_exit_contract(contract)
    assert contract["sha256"] == before_sha


def test_revision_requires_exact_account_lifecycle_original_sha_and_owner(monkeypatch):
    contract = _legacy(monkeypatch)
    base = _position(contract)
    for mismatch in (
        {"account_id":"other"}, {"lifecycle_id":"other"},
        {"source_contract_sha256":"0"*64}, {"strategy_owner":"TQQQ_INFINITE"},
        {"_db_verified":False}, {"revision_code":"OTHER"}, {"hard_stop":0.08},
    ):
        p = dict(base, _verified_pb1_stop_revision=_revision(contract, **mismatch))
        assert validated_revision(p) == {}
        assert contract_exit_config(p)["hard_stop"] == 0.08


def test_infinite_and_unscoped_positions_never_get_pb1_revision(monkeypatch):
    contract = _legacy(monkeypatch)
    p = _position(contract)
    p["symbol"] = "TQQQ"
    p["_verified_pb1_stop_revision"] = _revision(contract, symbol="TQQQ")
    assert validated_revision(p) == {}
    p = _position(contract)
    p.pop("account_id")
    p["_verified_pb1_stop_revision"] = _revision(contract)
    assert validated_revision(p) == {}


def test_revision_batch_attachment_is_non_mutating_and_scoped(monkeypatch):
    contract = _legacy(monkeypatch)
    p = _position(contract)
    monkeypatch.setattr(
        "trader.us.stop_policy_revision.load_approved_revisions",
        lambda positions, **kwargs: {("AMD","amd-life","practice-account"):_revision(contract)},
    )
    result = attach_approved_revisions([p])
    assert "_verified_pb1_stop_revision" not in p
    assert contract_exit_config(result[0])["hard_stop"] == 0.06
