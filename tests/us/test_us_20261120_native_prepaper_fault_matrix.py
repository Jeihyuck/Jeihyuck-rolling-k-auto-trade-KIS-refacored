"""US_STANDARD pre-paper BUY durability / ACK loss / verified original policy.

All broker responses are synthetic. Requests are intercepted at the HTTP
boundary: no actual KIS network, live orders or production DB mutations.
These are additional regressions, not WSL/practice execution certification.
"""
from __future__ import annotations

from copy import deepcopy
from unittest.mock import patch

import pytest

from trader.us.execution.order_router import route_order
from trader.us.entry_exit_contract import (
    contract_day_config,
    contract_exit_config,
    contract_profit_capture,
    extract_us_entry_exit_contract,
    us_entry_exit_contract_integrity_state,
)
from trader.us.pb1.us_exit_position_resolver import _enrich_single_position
from tests.us.test_us_route_order_claim_boundary import (
    _intent, _native_kis_client, _route_fixture,
)
from tests.us.test_us_20261120_original_contract_resolver import (
    RAW, _identity, _position, _proof, _source,
)


def _native_fixture(monkeypatch):
    _engine, claim_repo, _ = _route_fixture(monkeypatch)
    http = []
    client = _native_kis_client(monkeypatch, http)
    return claim_repo, client, http


def test_buy_intent_failed_durable_save_is_before_http_fail_closed(monkeypatch):
    from trader.us.db import repos
    claim_repo, client, http = _native_fixture(monkeypatch)
    monkeypatch.setattr(repos, "save_order_intent", lambda *a, **kw: False)
    result = route_order(
        _intent("2026-11-20", client_order_key="prepaper-intent-fail"),
        kis_client=client,
    )
    assert result["status"] == "INVALID_ORDER_IDENTITY"
    assert result["broker_submit"] is False
    assert not http
    assert claim_repo.health()["unresolved_execution_actions"] == 0


def test_broker_post_timeout_never_reissues_same_or_next_day_native(monkeypatch):
    claim_repo, client, http = _native_fixture(monkeypatch)
    key = "prepaper-post-timeout"
    first = route_order(_intent("2026-11-20", client_order_key=key), kis_client=client)
    assert first["status"] == "BROKER_SUBMIT_RESULT_UNKNOWN"
    assert first["requires_reconcile"] is True
    assert first["retry_order"] is False
    assert http == ["HTTP"]
    for date in ("2026-11-20", "2026-11-23"):
        blocked = route_order(_intent(date, client_order_key=key), kis_client=client)
        assert blocked["status"] in {
            "ORDER_FENCED_UNRESOLVED_ACTION", "WARN_DUPLICATE_EXIT_BLOCKED",
        }
        assert blocked.get("broker_submit") is False
        assert http == ["HTTP"]
    assert claim_repo.health()["unresolved_execution_actions"] == 1


def test_native_kis_ack_db_failure_is_not_broker_reject_or_second_post(monkeypatch):
    import requests
    from trader.us.db import repos

    claim_repo, client, http = _native_fixture(monkeypatch)

    class SuccessfulAck:
        def raise_for_status(self):
            pass

        def json(self):
            return {"rt_cd": "0", "order_no": "PAPER-FAKE-ACK-1"}

    def fake_post(*args, **kwargs):
        http.append("HTTP")
        return SuccessfulAck()

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(repos, "save_order_ack", lambda *a, **kw: False)
    key = "prepaper-native-ack-db-failure"
    first = route_order(_intent("2026-11-20", client_order_key=key), kis_client=client)
    assert first["status"] == "ACK_DB_FAILED"
    assert first["kis_ack"] is True
    assert first["ack_db_saved"] is False
    assert first["broker_submit"] is True
    assert first["retry_order"] is False
    assert first["requires_reconcile"] is True
    assert http == ["HTTP"]

    later = route_order(
        _intent("2026-11-23", client_order_key=key), kis_client=client,
    )
    assert later["status"] == "ORDER_FENCED_UNRESOLVED_ACTION"
    assert later.get("broker_submit") is False
    assert http == ["HTTP"], "ACK DB failure must never create a second BUY"
    assert claim_repo.health()["unresolved_execution_actions"] == 1


def test_amgn_independently_verified_original_flows_to_swing_day_tp_consumers(monkeypatch):
    original = _source()
    before = _position(original)
    immutable_before = deepcopy(before)
    from trader.us.stop_policy_revision import validated_revision

    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("WSL_RUN_MARKET", "US")
    with patch("trader.account_state.get_account_key", return_value=RAW), (
        patch("trader.us.pb1.us_exit_position_resolver._identity_is_current",
              return_value=True)), (
        patch("trader.us.original_buy_lineage.load_verified_us_original_buy_scope",
              return_value=_proof(original))), (
        patch("trader.us.verified_original_contract_resolver._no_intervening_order_for_original",
              return_value=True)), (
        patch("trader.us.stop_policy_revision.validated_revision", return_value=None)
    ):
        position = _enrich_single_position(
            before, trade_date="2026-11-19", env="practice",
            preloaded_risk_candidates=[],
        )
        assert us_entry_exit_contract_integrity_state(
            position, position.get("meta"),
        ) == "VALID"
        assert position["_verified_original_policy_source"] == (
            "EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY"
        )
        assert extract_us_entry_exit_contract(position, position.get("meta")) == original
        management = original["management"]
        assert contract_exit_config(position) == dict(management.get("swing") or {})
        assert contract_day_config(position) == dict(management.get("day") or {})
        capture = contract_profit_capture(position)
        for key, value in (management.get("profit_capture") or {}).items():
            assert capture[key] == value
    assert before == immutable_before, "runtime copy must not replace durable BUY"


def test_unproven_original_cannot_authorize_any_policy_consumer(monkeypatch):
    original = _source()
    before = _position(original)
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("WSL_RUN_MARKET", "US")
    with patch("trader.account_state.get_account_key", return_value=RAW), (
        patch("trader.us.pb1.us_exit_position_resolver._identity_is_current",
              return_value=True)), (
        patch("trader.us.original_buy_lineage.load_verified_us_original_buy_scope",
              return_value=None)):
        position = _enrich_single_position(
            before, trade_date="2026-11-19", env="practice",
            preloaded_risk_candidates=[],
        )
    assert position.get("_verified_original_policy_source") is None
    assert us_entry_exit_contract_integrity_state(
        position, position.get("meta"),
    ) == "INVALID"
    assert extract_us_entry_exit_contract(position, position.get("meta")) == {}
    assert contract_day_config(position) == {}
    assert contract_profit_capture(position) == {}
