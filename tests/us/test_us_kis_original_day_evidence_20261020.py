"""Read-only operator KIS audit: every result comes from real identity rules."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _audit():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "us_kis_original_day_evidence.py"
    spec = importlib.util.spec_from_file_location("us_kis_original_day_evidence", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def fixture():
    claim = {
        "trade_date": "2026-10-14", "strategy_owner": "US_STANDARD",
        "action_state": "IN_FLIGHT", "attempt_id": "submit-1",
        "client_order_key": "net-exit", "requested_qty": 2,
        "symbol": "NET", "side": "SELL", "local_status": "FILLED",
        "local_qty": 2,
    }
    journal = [
        {**claim, "event_type": "BROKER_SUBMIT_STARTED",
         "submit_attempt_id": "submit-1", "qty": 2},
        {**claim, "event_type": "BROKER_ACK_RECEIVED",
         "submit_attempt_id": "submit-1", "qty": 2,
         "broker_order_no": "0000001234"},
    ]
    scope = {
        "verified": True, "source": "kis_authenticated_order_book",
        "query_trade_date": "2026-10-14", "market": "US",
        "account_id": "verified-account-fingerprint", "env": "practice",
    }
    broker = {
        "trade_date": "2026-10-14", "order_no": "0000001234",
        "symbol": "NET", "side": "SELL", "requested_qty": 2,
        "filled_qty": 2, "remaining_qty": 0, "avg_price": 100.25,
        "status": "FILLED",
    }
    return claim, journal, scope, broker


def test_exact_broker_full_fill_proves_terminal_but_never_releases_claim(fixture):
    claim, journal, scope, broker = fixture
    result = _audit().verify_claim(claim, journal, [broker], scope)
    assert result["proof_status"] == "VERIFIED_FULL_FILL", result
    assert result["actual_filled_qty"] == 2
    assert result["actual_avg_price"] == 100.25
    assert result["terminal_proven"] is True
    assert result["fence_release_authorized"] is False
    assert result["database_mutated"] is False


@pytest.mark.parametrize("mutate, expected", [
    (lambda b: {**b, "avg_price": 0}, "MISSING_ACTUAL_FILL_PRICE"),
    (lambda b: {**b, "symbol": "META"}, "NO_EXACT_ORDER_IN_KIS_HISTORY"),
    (lambda b: {**b, "requested_qty": 3}, "NO_EXACT_ORDER_IN_KIS_HISTORY"),
    (lambda b: {**b, "filled_qty": None}, "NO_EXACT_ORDER_IN_KIS_HISTORY"),
    (lambda b: {**b, "normalization_result": "quarantined"}, "NO_EXACT_ORDER_IN_KIS_HISTORY"),
    (lambda b: {**b, "status": "OPEN"}, "BROKER_ORDER_STILL_ACTIVE"),
])
def test_missing_or_conflicting_broker_proof_cannot_release_fence(fixture, mutate, expected):
    claim, journal, scope, broker = fixture
    result = _audit().verify_claim(claim, journal, [mutate(broker)], scope)
    assert result["proof_status"] == expected or result["reason"] == expected, result
    assert result["fence_release_authorized"] is False


def test_actual_partial_cancel_keeps_semantic_unfilled_remainder(fixture):
    claim, journal, scope, broker = fixture
    proof = _audit().verify_claim(
        claim, journal, [{**broker, "filled_qty": 1,
                          "avg_price": 90.0, "status": "CANCELLED"}], scope)
    assert proof["proof_status"] == "VERIFIED_PARTIAL_TERMINAL"
    assert proof["remaining_target_qty"] == 1
    assert proof["terminal_proven"] is True
    assert proof["fence_release_authorized"] is False


def test_zero_fill_cancel_is_explicit_terminal_evidence_not_history_absence(fixture):
    claim, journal, scope, broker = fixture
    mod = _audit()
    cancel = mod.verify_claim(claim, journal, [
        {**broker, "filled_qty": 0, "avg_price": 0.0, "status": "CANCELLED"}
    ], scope)
    assert cancel["proof_status"] == "VERIFIED_ZERO_FILL_TERMINAL"
    empty = mod.verify_claim(claim, journal, [], scope)
    assert empty["proof_status"] == "UNRESOLVED"
    assert empty["reason"] == "NO_EXACT_ORDER_IN_KIS_HISTORY"
    assert empty["terminal_proven"] is False


def test_ambiguous_same_order_observation_fails_closed(fixture):
    claim, journal, scope, broker = fixture
    result = _audit().verify_claim(claim, journal, [
        broker, {**broker, "filled_qty": 1, "status": "PARTIALLY_FILLED"}
    ], scope)
    assert result["reason"] == "CONFLICTING_BROKER_OBSERVATIONS"
    assert result["terminal_proven"] is False


@pytest.mark.parametrize("scope_override", [
    {"verified": False}, {"query_trade_date": "2026-10-15"},
    {"account_id": ""}, {"market": "KR"},
])
def test_identity_and_original_day_provenance_required(fixture, scope_override):
    claim, journal, scope, broker = fixture
    result = _audit().verify_claim(
        claim, journal, [broker], {**scope, **scope_override})
    assert result["reason"] == "BROKER_ACCOUNT_OR_DATE_SCOPE_NOT_VERIFIED"
    assert result["terminal_proven"] is False


def test_query_batched_by_original_trade_date_not_by_claim(fixture):
    claim, journal, scope, broker = fixture
    mod = _audit()
    calls = []
    class Broker:
        def get_today_orders(self, *, trade_date):
            calls.append(("orders", trade_date))
            return [broker]
        def get_order_lookup_provenance(self, trade_date):
            calls.append(("scope", trade_date))
            return scope
    reports = mod.collect_evidence(
        [claim, claim], provider=Broker(),
        event_loader=lambda day: journal,
    )
    assert len(reports) == 2
    assert all(row["proof_status"] == "VERIFIED_FULL_FILL" for row in reports)
    assert calls == [("orders", "2026-10-14"), ("scope", "2026-10-14")]


def test_unavailable_kis_preserves_fence_instead_of_reporting_success(fixture):
    claim, journal, _, _ = fixture
    class Broker:
        def get_today_orders(self, *, trade_date):
            raise RuntimeError("KIS endpoint unavailable")
        def get_order_lookup_provenance(self, trade_date):
            pytest.fail("must not manufacture provenance after failed HTTP")
    out = _audit().collect_evidence(
        [claim], provider=Broker(), event_loader=lambda day: journal,
    )
    assert out[0]["proof_status"] == "UNRESOLVED"
    assert out[0]["terminal_proven"] is False


@pytest.mark.parametrize("another_attempt, another_order", [
    (False, "0000009999"),
    (True, "0000001234"),
])
def test_historical_multi_order_or_retry_conflict_cannot_prove_recovery(
    fixture, another_attempt, another_order
):
    claim, journal, scope, broker = fixture
    other = {
        **journal[1], "broker_order_no": another_order,
        "submit_attempt_id": "submit-2" if another_attempt else "submit-1",
    }
    records = list(journal)
    if another_attempt:
        records.append({**journal[0], "submit_attempt_id": "submit-2"})
    records.append(other)
    result = _audit().verify_claim(claim, records, [broker], scope)
    assert result["reason"] == "MULTI_ATTEMPT_OR_BROKER_ORDER_COLLISION"
    assert result["terminal_proven"] is False
    assert result["fence_release_authorized"] is False


@pytest.mark.parametrize("local_us_day,kst_clock,domestic_day,expected", [
    ("20261014","011210","20261015","2026-10-14"),
    ("20261013","020447","20261014","2026-10-13"),
    ("20261015","233857","20261015","2026-10-15"),
    ("20261006","001427","20261007","2026-10-06"),
])
def test_real_kis_market_day_is_not_misread_as_kst_calendar_date(
    local_us_day, kst_clock, domestic_day, expected
):
    from trader.us.execution.kis_us_client import KisUSClient
    from trader.us.data_provider import normalize_us_order_status_row
    raw = {
        "ord_dt": local_us_day, "ord_tmd": kst_clock,
        "dmst_ord_dt": domestic_day, "odno": "00000012345",
        "pdno": "TEST", "sll_buy_dvsn_cd": "01",
        "ft_ord_qty": "2", "ft_ccld_qty": "2", "nccs_qty": "0",
        "ft_ccld_unpr3": "283.86",
    }
    assert KisUSClient._broker_order_us_trade_date(raw) == expected
    assert KisUSClient._filter_broker_rows_for_us_trade_date(
        [raw], expected
    ) == [raw]
    assert KisUSClient._filter_broker_rows_for_us_trade_date(
        [raw], "2026-10-15" if expected != "2026-10-15" else "2026-10-14"
    ) == []
    normalized = normalize_us_order_status_row(raw)
    assert normalized["us_trade_date"] == expected
    assert normalized["normalization_result"] == "normalized"
    assert normalized["filled_qty"] == 2
    assert normalized["avg_price"] == 283.86
