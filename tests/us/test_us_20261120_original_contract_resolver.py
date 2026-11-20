"""AMGN 11/10 runtime derivative mismatch versus exact original (synthetic).

No network, broker calls, production DB reads/writes or fabricating fills.
The production lookup independently verifies native cumulative KIS evidence.
"""
from __future__ import annotations

import copy
import hashlib
from unittest.mock import patch

from trader.us.entry_exit_contract import (
    build_us_entry_exit_contract, verify_us_entry_exit_contract,
    us_entry_exit_contract_integrity_state,
)
from trader.us.verified_original_contract_resolver import (
    restore_verified_us_original_contract,
)
from trader.us.pb1.us_exit_position_resolver import _enrich_single_position

RAW = "practice-amgn-test-account"
ACCOUNT_HASH = hashlib.sha256(RAW.encode()).hexdigest()
EPOCH = "amgn-test-trading-epoch"
PORTFOLIO = "amgn-test-portfolio"
CYCLE = "amgn-test-original-cycle"


def _source():
    contract = build_us_entry_exit_contract({
        "symbol": "AMGN", "strategy_owner": "US_STANDARD",
        "entry_strategy": "us_pb1", "entry_style_selected": "ENTRY_PULLBACK",
    })
    assert verify_us_entry_exit_contract(contract)
    return contract


def _position(contract):
    altered = copy.deepcopy(contract)
    altered["entry_provenance"]["entry_reason"] = "MUTATED_DERIVATIVE_COPY"
    assert not verify_us_entry_exit_contract(altered)
    return {
        "symbol": "AMGN", "side": "BUY", "strategy_owner": "US_STANDARD",
        "qty": 6, "entry_price": 330, "entry_price_source": "KIS_BROKER",
        "current_price": 325, "account_id": ACCOUNT_HASH,
        "broker_account_hash": ACCOUNT_HASH,
        "trading_epoch_id": EPOCH, "portfolio_epoch_id": PORTFOLIO,
        "position_cycle_id": CYCLE, "position_lifecycle_id": CYCLE,
        "opened_trade_date": "2026-11-10",
        "meta": {
            "account_id": RAW, "broker_account_hash": ACCOUNT_HASH,
            "strategy_owner": "US_STANDARD",
            "trading_epoch_id": EPOCH,
            "portfolio_epoch_id": PORTFOLIO,
            "position_cycle_id": CYCLE, "position_lifecycle_id": CYCLE,
            "entry_exit_contract": altered,
            "entry_exit_contract_sha256": contract["sha256"],
            "entry_exit_contract_version": contract["version"],
        },
    }


def _identity():
    return {
        "env": "practice", "account_id": RAW, "trading_epoch_id": EPOCH,
        "strategy_owner": "US_STANDARD", "position_lifecycle_id": CYCLE,
    }


def _proof(original):
    return {
        "symbol": "AMGN", "side": "BUY", "qty": 6,
        "strategy_owner": "US_STANDARD", "account_id": RAW,
        "trading_epoch_id": EPOCH, "portfolio_epoch_id": PORTFOLIO,
        "position_cycle_id": CYCLE, "position_lifecycle_id": CYCLE,
        "order_no": "SYNTHETIC_ORDER", "client_order_key": "synthetic-original",
        "filled_at": "2026-11-10T15:10:00+00:00",
        "meta": {
            "entry_exit_contract": copy.deepcopy(original),
            "lineage_resolution": "EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY",
        },
    }


def test_runtime_bad_copy_recovers_only_separately_proven_original():
    original = _source()
    position = _position(original)
    assert us_entry_exit_contract_integrity_state(position, position["meta"]) == "INVALID"
    calls = []
    def lookup(**kwargs):
        calls.append(kwargs)
        return _proof(original)
    restored, reason = restore_verified_us_original_contract(
        position, identity=_identity(), current_trade_date="2026-11-19",
        original_lookup=lookup,
        no_intervening_lookup=lambda **_: True,
    )
    assert reason == "RESTORED_FROM_VERIFIED_NATIVE_ORIGINAL"
    assert restored is not position
    assert restored["meta"]["entry_exit_contract"] == original
    assert verify_us_entry_exit_contract(restored["meta"]["entry_exit_contract"])
    assert position["meta"]["entry_exit_contract"] != original  # no mutation
    assert calls[0]["trade_date"] == "2026-11-10"
    assert calls[0]["qty"] == 6
    assert calls[0]["identity"]["account_id"] == RAW


def test_cross_account_epoch_cycle_and_qty_never_recover():
    original = _source()
    for changed in (
        {"qty": 5},
        {"position_cycle_id": "other-cycle"},
        {"portfolio_epoch_id": "other-portfolio"},
        {"trading_epoch_id": "other-epoch"},
        {"opened_trade_date": "2026-11-30"},
    ):
        p = _position(original)
        p.update(changed)
        original_lookup_calls = []
        def lookup(**kwargs):
            original_lookup_calls.append(kwargs)
            return _proof(original)
        restored, reason = restore_verified_us_original_contract(
            p, identity=_identity(), current_trade_date="2026-11-19",
            original_lookup=lookup,
        no_intervening_lookup=lambda **_: True,
        )
        assert restored is None, (changed, reason)


def test_disagreeing_copy_digest_or_version_not_unconditionally_adopted():
    original = _source()
    for altered in (
        {"entry_exit_contract_sha256": "different_sha"},
        {"entry_exit_contract_version": "other_v4"},
    ):
        p = _position(original)
        p["meta"].update(altered)
        restored, reason = restore_verified_us_original_contract(
            p, identity=_identity(), current_trade_date="2026-11-19",
            original_lookup=lambda **_: _proof(original),
            no_intervening_lookup=lambda **_: True,
        )
        assert restored is None, (altered, reason)


def test_invalid_independent_original_cannot_be_normalized_into_truth():
    original = _source()
    untrusted = _proof(original)
    untrusted["meta"]["entry_exit_contract"]["management"]["book"] = "TAMPERED"
    restored, reason = restore_verified_us_original_contract(
        _position(original), identity=_identity(), current_trade_date="2026-11-19",
        original_lookup=lambda **_: untrusted,
        no_intervening_lookup=lambda **_: True,
    )
    assert restored is None
    assert reason == "ORIGINAL_POLICY_HASH_INVALID"


def test_tqqq_and_already_valid_policy_are_not_recovered():
    original = _source()
    p = _position(original)
    p["symbol"] = "TQQQ"
    p["strategy_owner"] = "TQQQ_INFINITE"
    assert restore_verified_us_original_contract(
        p, identity=_identity(), current_trade_date="2026-11-19",
        original_lookup=lambda **_: _proof(original),
            no_intervening_lookup=lambda **_: True,
    )[0] is None
    p = _position(original)
    p["meta"]["entry_exit_contract"] = original
    assert restore_verified_us_original_contract(
        p, identity=_identity(), current_trade_date="2026-11-19",
        original_lookup=lambda **_: _proof(original),
            no_intervening_lookup=lambda **_: True,
    )[1] == "NO_INVALID_RUNTIME_COPY"


def test_exit_enrichment_reads_proven_source_but_does_not_change_broker_qty():
    original = _source()
    p = _position(original)
    with patch.dict("os.environ", {"KIS_ENV": "practice", "WSL_RUN_MARKET": "US"}):
        with patch("trader.account_state.get_account_key", return_value=RAW), (
             patch("trader.us.pb1.us_exit_position_resolver._identity_is_current", return_value=True)), (
             patch("trader.us.original_buy_lineage.load_verified_us_original_buy_scope",
                   return_value=_proof(original))), (
             patch("trader.us.verified_original_contract_resolver._no_intervening_order_for_original",
                   return_value=True)):

            restored = _enrich_single_position(
                p, trade_date="2026-11-19", env="practice",
                preloaded_risk_candidates=[],
            )
    assert restored["qty"] == 6
    assert restored["_verified_original_policy_source"] == "EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY"
    assert restored["position_lifecycle_view"]["contract_integrity"] == "VALID"
    assert verify_us_entry_exit_contract(restored["meta"]["entry_exit_contract"])
    assert p["meta"]["entry_exit_contract"] != original


def test_equal_current_shares_after_intervening_order_are_not_safe():
    original = _source()
    restored, reason = restore_verified_us_original_contract(
        _position(original), identity=_identity(), current_trade_date="2026-11-19",
        original_lookup=lambda **_: _proof(original),
        no_intervening_lookup=lambda **_: False,
    )
    assert restored is None
    assert reason == "ORIGINAL_CYCLE_INTERVENING_ORDER_UNVERIFIED"
