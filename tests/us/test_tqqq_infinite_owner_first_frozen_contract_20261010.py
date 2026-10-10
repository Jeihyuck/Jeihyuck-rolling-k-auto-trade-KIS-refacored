"""A signed US_STANDARD contract cannot be inherited by TQQQ_INFINITE."""
import copy
import json

import pytest

from trader.us.entry_exit_contract import (
    build_us_entry_exit_contract,
    extract_us_entry_exit_contract,
    us_entry_exit_contract_integrity_state,
    verify_us_entry_exit_contract,
    contract_exit_config,
    contract_profit_capture,
)


def _signed_standard():
    frozen = build_us_entry_exit_contract({
        "symbol": "AAPL",
        "strategy_owner": "US_STANDARD",
        "entry_reason": "ENTRY_MOMENTUM",
        "entry_style_selected": "ENTRY_MOMENTUM",
    })
    assert verify_us_entry_exit_contract(frozen)
    return frozen


@pytest.mark.parametrize("embed", ["direct", "meta", "json"])
def test_tqqq_rejects_inherited_signed_standard_contract(embed):
    standard = _signed_standard()
    payload = copy.deepcopy(standard)
    if embed == "direct":
        position = {"symbol": "TQQQ", "entry_exit_contract": payload}
    elif embed == "meta":
        position = {"symbol": "TQQQ", "meta": {"entry_exit_contract": payload}}
    else:
        position = {"symbol": "TQQQ", "meta": {
            "entry_exit_contract": json.dumps(payload),
            "entry_exit_contract_sha256": payload["sha256"],
        }}
    assert build_us_entry_exit_contract(position) == {}
    assert extract_us_entry_exit_contract(position) == {}
    assert us_entry_exit_contract_integrity_state(position) == "INVALID"
    assert contract_exit_config(position) == {}
    assert contract_profit_capture(position) == {}


def test_any_explicit_infinite_owner_source_vetoes_standard_contract():
    standard = _signed_standard()
    source = {"symbol": "AAPL", "entry_exit_contract": standard}
    for forbidden in (
        {"symbol": "TQQQ", "strategy_owner": "US_STANDARD"},
        {"symbol": "AAPL", "strategy_owner": "TQQQ_INFINITE"},
        {"meta": {"sleeve_id": "TQQQ_INFINITE"}},
    ):
        for ordered in ((source, forbidden), (forbidden, source)):
            assert build_us_entry_exit_contract(*ordered) == {}
            assert extract_us_entry_exit_contract(*ordered) == {}
            assert us_entry_exit_contract_integrity_state(*ordered) == "INVALID"


def test_tqqq_without_claim_is_not_mislabeled_as_corrupt_standard_contract():
    tqqq = {"symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE"}
    assert build_us_entry_exit_contract(tqqq) == {}
    assert extract_us_entry_exit_contract(tqqq) == {}
    assert us_entry_exit_contract_integrity_state(tqqq) == "NONE"


def test_valid_standard_reuse_and_digest_still_work():
    signed = _signed_standard()
    aapl = {"symbol": "AAPL", "strategy_owner": "US_STANDARD",
            "meta": {"entry_exit_contract": signed}}
    assert build_us_entry_exit_contract(aapl) == signed
    assert extract_us_entry_exit_contract(aapl) == signed
    assert us_entry_exit_contract_integrity_state(aapl) == "VALID"
    bad = copy.deepcopy(signed)
    bad["entry_provenance"]["entry_reason"] = "TAMPERED"
    damaged = {"symbol": "AAPL", "meta": {"entry_exit_contract": bad}}
    assert extract_us_entry_exit_contract(damaged) == {}
    assert us_entry_exit_contract_integrity_state(damaged) == "INVALID"
