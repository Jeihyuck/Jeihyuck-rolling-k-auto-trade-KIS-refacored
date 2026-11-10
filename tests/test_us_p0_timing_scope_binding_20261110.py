"""Regression: 2026-11-09 PB1 placeholder timing hash must bind final scope."""
from __future__ import annotations

import copy
import hashlib
import json
import unittest
from unittest.mock import patch

from trader.us.entry_timing.opportunity import (
    attach_opportunity_proof,
    pre_submit_timing_check,
)
from trader.us.execution.recovery_identity import (
    USRecoveryIdentityError,
    bind_original_us_order_scope,
)


def _intent(symbol="AMGN", key="buy-test-1", attempt="attempt-1",
            limit=100.01):
    symbol = symbol.upper()
    intent = {
        "symbol": symbol, "side": "BUY", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY",
        "position_lifecycle_id": f"ENTRY:US_STANDARD:{symbol}",
        "client_order_key": key, "submit_attempt_id": attempt,
        "trade_date": "2026-11-09", "limit_price": limit,
        "meta": {"position_lifecycle_id": f"ENTRY:US_STANDARD:{symbol}"},
    }
    decision = {
        "state": "READY", "decision_price": 100.0,
        "max_entry_price": 101.0, "structural_stop_reference": 94.0,
    }
    decision["evidence_sha256"] = hashlib.sha256(
        json.dumps(decision, sort_keys=True, separators=(",", ":"),
                   allow_nan=False).encode()
    ).hexdigest()
    attach_opportunity_proof(intent, decision, mode="ENFORCE", decision_epoch=1000.0)
    return intent


def _scope(intent):
    with patch(
        "trader.us.execution.recovery_identity.current_us_portfolio_epoch",
        return_value="portfolio-test-epoch",
    ):
        return bind_original_us_order_scope(
            intent, conn=object(), env="practice",
            account_key="practice-account-test", trading_epoch_id="trading-test-epoch",
        )


def _check(intent, epoch=1002.0):
    return pre_submit_timing_check(
        intent, now_epoch=epoch,
        fresh_ws_quote={"received_at": epoch, "last": 100.0,
                        "ask": 100.0, "bid": 99.95},
    )


class PB1TimingScopeRegression(unittest.TestCase):
    def test_authentic_producer_placeholder_resolves_before_persistence(self):
        intent = _intent()
        original = copy.deepcopy(intent)
        self.assertEqual(_check(intent)[0], True)
        _scope(intent)
        self.assertEqual(intent["position_cycle_id"], intent["position_lifecycle_id"])
        self.assertNotEqual(intent["position_lifecycle_id"], original["position_lifecycle_id"])
        proof = intent["meta"]["us_entry_opportunity_v1"]
        self.assertEqual(proof["submission_binding_version"], 2)
        for key in ("evidence_sha256", "decision_epoch", "max_age_sec", "max_entry_price"):
            self.assertEqual(proof[key], original["meta"]["us_entry_opportunity_v1"][key])
        self.assertTrue(_check(intent)[0])
        # Exact same scope re-validation must not re-sign a previous decision.
        signed = proof["submission_binding_sha256"]
        _scope(intent)
        self.assertEqual(intent["meta"]["us_entry_opportunity_v1"]["submission_binding_sha256"], signed)

    def test_28_synthetic_attempts_are_not_bound_to_placeholder(self):
        # Synthetic identity matrix; this is NOT replay of the 28 production rows.
        for i in range(28):
            symbol = ("AMGN", "HUM", "MSFT", "AMZN", "RSG", "V")[i % 6]
            with self.subTest(i=i):
                intent = _intent(symbol, f"key-{i}", f"attempt-{i}")
                _scope(intent)
                self.assertEqual(_check(intent), (True, "TIMING_FRESH_PRICE_CONFIRMED"))

    def test_tampered_signal_and_original_binding_never_reseal(self):
        for field, value in (
            ("client_order_key", "forged-key"),
            ("strategy_owner", "OTHER"),
            ("position_lifecycle_id", "ENTRY:US_STANDARD:WRONG"),
        ):
            with self.subTest(field=field):
                intent = _intent()
                intent[field] = value
                with self.assertRaises((ValueError, USRecoveryIdentityError)):
                    _scope(intent)

    def test_tampered_signal_hash_never_reseal(self):
        intent = _intent()
        intent["meta"]["us_entry_opportunity_v1"]["max_entry_price"] = 999.0
        with self.assertRaises((ValueError, USRecoveryIdentityError)):
            _scope(intent)

    def test_final_scope_tampering_expires_or_violates_cap(self):
        intent = _intent()
        _scope(intent)
        for field, forged in (
            ("client_order_key", "different-key"),
            ("portfolio_epoch_id", "wrong-epoch"),
            ("position_cycle_id", "wrong-cycle"),
            ("submit_attempt_id", "wrong-attempt"),
        ):
            with self.subTest(field=field):
                altered = copy.deepcopy(intent)
                altered[field] = forged
                self.assertEqual(_check(altered)[1], "TIMING_BINDING_INVALID")
        self.assertEqual(_check(intent, epoch=1500)[1], "TIMING_SIGNAL_EXPIRED")
        beyond_cap = _intent(limit=100.005)
        beyond_cap["meta"]["us_entry_opportunity_v1"]["max_entry_price"] = 100.005
        # Create a valid signed decision with a cap below executable 100.01:
        decision = {"state": "READY", "decision_price": 100.0,
                    "max_entry_price": 100.005,
                    "structural_stop_reference": 94.0}
        decision["evidence_sha256"] = hashlib.sha256(json.dumps(
            decision, sort_keys=True, separators=(",", ":"),
            allow_nan=False,
        ).encode()).hexdigest()
        attach_opportunity_proof(
            beyond_cap, decision, mode="ENFORCE", decision_epoch=1000,
        )
        _scope(beyond_cap)
        self.assertEqual(_check(beyond_cap)[1], "TIMING_PRICE_CAP_VIOLATED")


if __name__ == "__main__":
    unittest.main()
