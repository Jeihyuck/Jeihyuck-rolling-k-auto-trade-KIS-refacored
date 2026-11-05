"""No retroactive v3 conversion; explicit practice-only BUY admission."""
from trader.us.entry_exit_contract import (
    build_us_entry_exit_contract, verify_us_entry_exit_contract,
    extract_us_entry_exit_contract, US_SWING_V3_CONTRACT_VERSION,
)
from trader.us.pb1.swing_runner_exit_v3 import V3_RUNNER_POLICY


def _new(symbol="SPY"):
    return {"symbol": symbol, "strategy_owner": "US_STANDARD",
            "book": "SWING_BOOK", "horizon": "SWING_CARRY",
            "position_action": "NEW_POSITION_BUY"}


def test_default_no_v3(monkeypatch):
    monkeypatch.delenv("US_SWING_EXIT_MODE", raising=False)
    assert build_us_entry_exit_contract(_new())["version"] == "us_entry_exit_contract_v2"


def test_explicit_approved_new_practice_buy_only(monkeypatch):
    monkeypatch.setenv("US_SWING_EXIT_MODE", "V3_ACTIVE_PRACTICE")
    monkeypatch.setenv("US_SWING_V3_DEPLOY_APPROVED", "1")
    monkeypatch.setenv("KIS_ENV", "practice")
    v3 = build_us_entry_exit_contract(_new())
    assert verify_us_entry_exit_contract(v3)
    assert v3["version"] == US_SWING_V3_CONTRACT_VERSION
    assert v3["management"]["runner_exit"] == V3_RUNNER_POLICY
    assert len(v3["management"]["profit_capture"]["stages"]) == 3
    assert extract_us_entry_exit_contract({"entry_exit_contract": v3}) == v3
    existing = build_us_entry_exit_contract({**_new(), "entry_exit_contract": v3})
    assert existing == v3
    assert build_us_entry_exit_contract({**_new(), "position_action": "ADD_TO_EXISTING_BUY"})["version"] == "us_entry_exit_contract_v2"
    assert build_us_entry_exit_contract({**_new(), "book": "DAY_BOOK"})["version"] == "us_entry_exit_contract_v2"
    assert build_us_entry_exit_contract(_new("TQQQ")) == {}


def test_real_or_unapproved_or_shadow_never_grants_new_v3(monkeypatch):
    monkeypatch.setenv("US_SWING_EXIT_MODE", "V3_ACTIVE_PRACTICE")
    monkeypatch.setenv("US_SWING_V3_DEPLOY_APPROVED", "0")
    monkeypatch.setenv("KIS_ENV", "practice")
    assert build_us_entry_exit_contract(_new())["version"] == "us_entry_exit_contract_v2"
    monkeypatch.setenv("US_SWING_V3_DEPLOY_APPROVED", "1")
    monkeypatch.setenv("KIS_ENV", "real")
    assert build_us_entry_exit_contract(_new())["version"] == "us_entry_exit_contract_v2"
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_SWING_EXIT_MODE", "V3_SHADOW")
    assert build_us_entry_exit_contract(_new())["version"] == "us_entry_exit_contract_v2"


def test_policy_tampering_is_invalid(monkeypatch):
    monkeypatch.setenv("US_SWING_EXIT_MODE", "V3_ACTIVE_PRACTICE")
    monkeypatch.setenv("US_SWING_V3_DEPLOY_APPROVED", "1")
    monkeypatch.setenv("KIS_ENV", "practice")
    v3 = build_us_entry_exit_contract(_new())
    import copy
    tampered = copy.deepcopy(v3)
    tampered["management"]["runner_exit"]["partial_giveback"] = .40
    assert not verify_us_entry_exit_contract(tampered)
