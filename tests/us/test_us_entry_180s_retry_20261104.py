"""Regression gates for 180-second US_STANDARD BUY signals and safe retry."""
from __future__ import annotations

import pytest

from trader.us.db import repos


@pytest.mark.parametrize(
    "prior, broker, attempts, expected, reason",
    [
        ([], False, {}, True, "FIRST_DECISION"),
        ([{"client_order_key": "first"}], False,
         {"first": [{"attempt_state": "ABANDONED_PRE_SUBMIT", "authoritative": True}]},
         True, "ALL_PRIOR_ATTEMPTS_PROVEN_UNSENT"),
        ([{"client_order_key": "first"}], True,
         {"first": [{"attempt_state": "ABANDONED_PRE_SUBMIT", "authoritative": True}]},
         False, "US_BUY_RETRY_BROKER_ORDER_EXISTS"),
        ([{"client_order_key": "first"}], False, {},
         False, "US_BUY_RETRY_PREVIOUS_ATTEMPT_UNVERIFIED"),
        ([{"client_order_key": "first"}], False,
         {"first": [{"attempt_state": "SUBMITTED", "authoritative": False}]},
         False, "US_BUY_RETRY_PREVIOUS_ATTEMPT_UNVERIFIED"),
        ([{"client_order_key": "first"}], False,
         {"first": [{"attempt_state": "ABANDONED_PRE_SUBMIT", "authoritative": False}]},
         False, "US_BUY_RETRY_PREVIOUS_ATTEMPT_UNVERIFIED"),
        ([{"client_order_key": "first"}, {"client_order_key": "second"}], False,
         {"first": [{"attempt_state": "ABANDONED_PRE_SUBMIT", "authoritative": True}],
          "second": [{"attempt_state": "ACKED", "authoritative": True}]},
         False, "US_BUY_RETRY_PREVIOUS_ATTEMPT_UNVERIFIED"),
        ([{"client_order_key": "first"}, {"client_order_key": "second"}], False,
         {"first": [{"attempt_state": "ABANDONED_PRE_SUBMIT", "authoritative": True}],
          "second": [{"attempt_state": "ABANDONED_PRE_SUBMIT", "authoritative": True}]},
         True, "ALL_PRIOR_ATTEMPTS_PROVEN_UNSENT"),
        ([{"client_order_key": "first"}], False,
         {"first": [
             {"attempt_state": "ABANDONED_PRE_SUBMIT", "authoritative": True},
             {"attempt_state": "UNKNOWN_AFTER_HTTP", "authoritative": False},
         ]},
         False, "US_BUY_RETRY_PREVIOUS_ATTEMPT_UNVERIFIED"),
    ],
)
def test_fresh_buy_requires_every_prior_attempt_proven_unsent(
    prior, broker, attempts, expected, reason,
):
    assert repos._us_standard_buy_retry_evidence(
        prior, broker_order_present=broker, attempts_by_key=attempts,
    ) == (expected, reason)


def test_database_unavailable_never_issues_a_new_order_identity(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    result = repos.resolve_us_standard_new_buy_key(
        symbol="XOM", trade_date="2026-11-03",
        base_key="first", decision_id="signed:1793721732.1",
    )
    assert result == {
        "allowed": False, "reason": "US_BUY_RETRY_LEDGER_UNAVAILABLE",
    }


def test_180_sec_signed_ttl_respects_legacy_120_sec_proof():
    from trader.us.entry_timing.opportunity import (
        MAX_BUY_SIGNAL_AGE_SEC, _submission_binding, attach_opportunity_proof,
        pre_submit_timing_check,
    )
    assert MAX_BUY_SIGNAL_AGE_SEC == 180.0
    decision = {
        "schema": "US_OPPORTUNITY_TRIAL_V1", "strategy_owner": "US_STANDARD",
        "entry_family": "ENTRY_PULLBACK", "state": "READY", "reason": "test",
        "facts": {}, "decision_price": 100.0, "max_entry_price": 100.1,
        "structural_stop_reference": 98.0, "structural_target_reference": 105.0,
        "risk_reward_reference": 2.0, "risk_reference_is_live_stop_order": False,
        "broker_authorized": False,
    }
    import json
    import hashlib
    decision["evidence_sha256"] = hashlib.sha256(json.dumps(
        decision, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode()).hexdigest()
    intent = {
        "symbol": "XOM", "side": "BUY", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY", "client_order_key": "first",
        "limit_price": 100.0, "meta": {},
    }
    attach_opportunity_proof(intent, decision, mode="ENFORCE", decision_epoch=1000)
    quote = {"received_at": 1174, "last": 100, "bid": 99.99, "ask": 100.0}
    assert pre_submit_timing_check(
        intent, now_epoch=1175, fresh_ws_quote=quote,
    ) == (True, "TIMING_FRESH_PRICE_CONFIRMED")
    assert pre_submit_timing_check(
        intent, now_epoch=1181, fresh_ws_quote=quote,
    )[1] == "TIMING_SIGNAL_EXPIRED"
    # An already-persisted older 120s proof does not get extended silently.
    proof = intent["meta"]["us_entry_opportunity_v1"]
    proof["max_age_sec"] = 120.0
    proof["submission_binding_sha256"] = _submission_binding(intent, proof)
    assert pre_submit_timing_check(
        intent, now_epoch=1130, fresh_ws_quote=quote,
    )[1] == "TIMING_SIGNAL_EXPIRED"


def test_live_opportunity_clock_is_not_stale_tick_start():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from trader.us.entry_timing.opportunity import (
        opportunity_observation_time,
    )
    ny = ZoneInfo("America/New_York")
    tick_start = datetime(2026, 11, 3, 9, 30, tzinfo=ny)
    # Historical deterministic replay gets the caller's exact timestamp.
    assert opportunity_observation_time(
        tick_start, live_clock=False,
    ) is tick_start
    # Live per-symbol evaluation obtains its own observed wall-clock time.
    before = datetime.now(ny)
    observed = opportunity_observation_time(tick_start, live_clock=True)
    after = datetime.now(ny)
    assert before <= observed <= after
    assert observed != tick_start
