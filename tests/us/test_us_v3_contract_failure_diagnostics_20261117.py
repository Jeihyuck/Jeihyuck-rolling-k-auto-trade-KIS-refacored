"""US v3: an invalid signed original BUY contract is diagnosed, never rewritten."""
from copy import deepcopy

import pytest

from trader.us.entry_exit_contract import (
    US_ENTRY_EXIT_CONTRACT_VERSION, _sha,
    explain_us_entry_exit_contract_integrity,
    us_entry_exit_contract_integrity_state,
    verify_us_entry_exit_contract,
)


def signed():
    obj = {
        "version": US_ENTRY_EXIT_CONTRACT_VERSION,
        "strategy_owner": "US_STANDARD",
        "sleeve_id": "US_STANDARD",
        "entry_provenance": {"entry_reason": "PULLBACK"},
        "management": {"book": "SWING_BOOK", "horizon": "SWING_CARRY"},
    }
    obj["sha256"] = _sha(obj)
    return obj


def test_valid_frozen_contract_reports_valid_and_never_mutates_source():
    c = signed()
    before = deepcopy(c)
    assert verify_us_entry_exit_contract(c)
    assert explain_us_entry_exit_contract_integrity(
        {"symbol": "XYZ", "meta": {"entry_exit_contract": c,
                                  "entry_exit_contract_sha256": c["sha256"]}}
    ) == "VALID"
    assert c == before


@pytest.mark.parametrize("tamper,reason", [
    (lambda c: c["management"].update(book="MUTATED"),
     "SIGNED_PAYLOAD_HASH_MISMATCH"),
    (lambda c: c.update(version="unknown"),
     "UNSUPPORTED_FROZEN_CONTRACT_VERSION"),
    (lambda c: c.update(strategy_owner="TQQQ_INFINITE"),
     "OWNER_NOT_US_STANDARD"),
    (lambda c: c.update(entry_provenance=None),
     "ORIGINAL_ENTRY_PROVENANCE_NOT_OBJECT"),
])
def test_tampered_contract_is_diagnostic_only_not_resigned(tamper, reason):
    c = signed()
    original_hash = c["sha256"]
    tamper(c)
    assert explain_us_entry_exit_contract_integrity(
        {"symbol": "ANY_US_TICKER", "entry_exit_contract": c}
    ) == reason
    assert us_entry_exit_contract_integrity_state(
        {"entry_exit_contract": c},
    ) == "INVALID"
    assert c["sha256"] == original_hash


def test_declared_sha_conflict_distinct_from_inner_payload_mismatch():
    c = signed()
    assert explain_us_entry_exit_contract_integrity(
        {"meta": {"entry_exit_contract": c,
                  "entry_exit_contract_sha256": "0" * 64}}
    ) == "DECLARED_DIGEST_CONFLICT_WITH_FROZEN_CONTRACT"


def test_infinite_owner_never_gets_standard_proof():
    c = signed()
    assert explain_us_entry_exit_contract_integrity(
        {"symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE",
         "entry_exit_contract": c},
    ) == "OWNER_SCOPE_TQQQ_INFINITE_CONFLICT"


def test_top_level_digest_only_wrapper_with_valid_nested_original_is_valid():
    c = signed()
    wrapped = {
        "symbol": "AMGN",
        "entry_exit_contract_sha256": c["sha256"],
        "meta": {
            "entry_exit_contract": c,
            "entry_exit_contract_sha256": c["sha256"],
        },
    }
    assert us_entry_exit_contract_integrity_state(wrapped) == "VALID"
    assert explain_us_entry_exit_contract_integrity(wrapped) == "VALID"
