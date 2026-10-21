"""2026-10-20 TQQQ TTL regression: broker-open and semantic remainder differ.

No live KIS, production database, order submit, or cancel is performed.
"""
from __future__ import annotations

from copy import deepcopy

import pytest

from trader.us.infinite.repository import InfiniteRepository


_ORDER = {
    "trade_date": "2026-10-20",
    "client_order_key": "TQQQ_INF_V3:cycle:2026-10-20:BUY",
    "order_no": "0000038776",
    "symbol": "TQQQ",
    "qty_requested": 3,
}


def _observation(*, filled: int = 0, broker_open: int | None = 0) -> dict:
    return {
        "order_no": "0000038776", "symbol": "TQQQ", "side": "BUY",
        "status": "CANCELLED", "requested_qty": 3,
        "filled_qty": filled,
        "filled_qty_present": True,
        "remaining_qty": broker_open,
        "avg_price": 70.0 if filled else None,
        "evidence_type": "TQQQ_TTL_CANCEL_ACK_ZERO_REMAINING",
    }


@pytest.mark.parametrize(
    ("filled", "expected_semantic"),
    [(0, 3), (1, 2), (3, 0)],
)
def test_cancel_preserves_distinct_broker_and_semantic_quantities(
    monkeypatch, filled, expected_semantic,
):
    from trader.us.db import repos
    captured = []

    def fake_apply(**kwargs):
        captured.append(deepcopy(kwargs))
        return {"status": "OK", "order_status": kwargs["broker_status"]}

    monkeypatch.setattr(repos, "apply_broker_order_observation", fake_apply)
    result = InfiniteRepository.apply_ttl_terminal_observation(
        object(), dict(_ORDER), _observation(filled=filled)
    )
    assert result["status"] == "OK"
    assert len(captured) == 1
    assert captured[0]["remaining_qty"] == expected_semantic
    assert captured[0]["raw_row"]["remaining_qty"] == 0
    assert captured[0]["filled_qty"] == filled
    assert captured[0]["raw_order_no"] == "0000038776"


@pytest.mark.parametrize(
    ("broker_open", "filled", "expected_reason"),
    [
        (None, 0, "cancel_broker_remaining_missing"),
        (3, 0, "cancel_still_broker_open"),
        (0, 4, "terminal_quantity_out_of_range"),
    ],
)
def test_cancel_fails_closed_without_terminal_quantity_proof(
    monkeypatch, broker_open, filled, expected_reason,
):
    from trader.us.db import repos
    monkeypatch.setattr(
        repos, "apply_broker_order_observation",
        lambda **kwargs: pytest.fail("unproven cancellation must not touch DB"),
    )
    actual = InfiniteRepository.apply_ttl_terminal_observation(
        object(), dict(_ORDER),
        _observation(filled=filled, broker_open=broker_open),
    )
    assert actual == {"status": "PENDING", "reason": expected_reason}


def test_kis_cancel_operation_has_original_reference_not_new_buy():
    from trader.us.data_provider import normalize_us_order_status_row
    normalized = normalize_us_order_status_row({
        "odno": "0000038961", "orgn_odno": "0000038776",
        "rvse_cncl_dvsn_cd": "02",
        "pdno": "TQQQ", "sll_buy_dvsn_cd": "02",
        "ord_qty": "3", "ft_ccld_qty": "0", "nccs_qty": "0",
    })
    assert normalized["side"] == "BUY"  # inherited KIS side
    assert normalized["broker_operation"] == "CANCEL"
    assert normalized["original_order_no"] == "0000038776"
    assert normalized["order_no"] == "0000038961"


def test_kis_zero_open_without_explicit_cancel_proof_is_not_discarded():
    from trader.us.data_provider import normalize_us_order_status_row
    normalized = normalize_us_order_status_row({
        "odno": "0000038961", "pdno": "TQQQ",
        "sll_buy_dvsn_cd": "02", "ord_qty": "3",
        "ft_ccld_qty": "0", "nccs_qty": "0",
    })
    assert normalized["broker_operation"] == "ORDER"


def test_close_matches_proven_cancel_operation_to_original(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution import order_journal, reconcile
    from trader.us.data_provider import normalize_us_order_status_row

    monkeypatch.setattr(
        repos, "load_pending_ack_orders_result",
        lambda *a, **kw: {"status": "OK", "orders": []},
    )
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [])
    monkeypatch.setattr(order_journal, "load_order_events", lambda day: [{
        "event_type": "BROKER_ACK_RECEIVED",
        "symbol": "TQQQ", "side": "BUY", "broker_order_no": "0000038776",
        "client_order_key": "cycle-A", "submit_attempt_id": "attempt-A",
        "qty_requested": 3,
    }])

    class Broker:
        def get_today_orders(self, day):
            return [
                normalize_us_order_status_row({
                    "odno": "0000038776", "pdno": "TQQQ", "sll_buy_dvsn_cd": "02",
                    "ord_qty": "3", "ft_ccld_qty": "0", "nccs_qty": "0",
                }),
                normalize_us_order_status_row({
                    "odno": "0000038961", "orgn_odno": "0000038776",
                    "rvse_cncl_dvsn_cd": "02", "pdno": "TQQQ",
                    "sll_buy_dvsn_cd": "02", "ord_qty": "3",
                    "ft_ccld_qty": "0", "nccs_qty": "0",
                }),
            ]

    attempts = reconcile._load_close_order_attempt_truth(
        provider=Broker(), trade_date="2026-10-20", env="practice"
    )
    assert len(attempts) == 1
    assert attempts[0]["order_no"] == "0000038776"
    assert attempts[0]["_broker_cancel_operation_nos"] == ["38961"]


def test_close_fails_closed_on_unlinked_cancel(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution import order_journal, reconcile
    monkeypatch.setattr(
        repos, "load_pending_ack_orders_result",
        lambda *a, **kw: {"status": "OK", "orders": []},
    )
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [])
    monkeypatch.setattr(order_journal, "load_order_events", lambda day: [])

    class Broker:
        def get_today_orders(self, day):
            return [{
                "broker_operation": "CANCEL",
                "order_no": "38961", "original_order_no": "38776",
                "symbol": "TQQQ", "side": "BUY",
            }]

    with pytest.raises(RuntimeError, match="broker_cancel_operation_original_attempt_unresolved"):
        reconcile._load_close_order_attempt_truth(
            provider=Broker(), trade_date="2026-10-20", env="practice"
        )


def test_cancelled_uncertain_claim_is_revisited_without_second_cancel():
    from datetime import datetime, timezone
    from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl

    order = {
        **_ORDER,
        "meta": {
            "tqqq_ttl_cancel_result": {
                "rt_cd": "0", "msg_cd": "40630000",
                "output": {"ODNO": "0000038961"},
            },
        },
    }
    observed = _observation()
    observed["status"] = "ACK_PENDING"  # original KIS row may lack terminal status
    submitted = []
    queried = []

    class Repo:
        def load_expired_open_buy_orders(self, **kwargs):
            return []

        def load_cancelled_uncertain_buy_orders(self, **kwargs):
            return [order]

        def apply_ttl_terminal_observation(self, original, evidence):
            submitted.append((original, evidence))
            return {"status": "OK", "order_status": "CANCELLED"}

    result = reconcile_tqqq_open_buy_ttl(
        repository=Repo(), now=datetime(2026, 10, 20, 15, tzinfo=timezone.utc),
        ttl_seconds=180,
        cancel_order=lambda **kw: pytest.fail("old cancelled order must not be cancelled again"),
        query_order=lambda **kw: queried.append(kw) or observed,
    )
    assert result["cancelled_claim_candidates"] == 1
    assert result["cancelled_claim_recovered"] == 1
    assert len(submitted) == 1
    assert submitted[0][1]["status"] == "CANCELLED"
    assert submitted[0][1]["cancel_broker_order_no"] == "0000038961"
    assert queried[0]["order_no"] == "0000038776"


def test_cancelled_uncertain_claim_retains_fence_without_broker_evidence():
    from datetime import datetime, timezone
    from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl

    class Repo:
        def load_expired_open_buy_orders(self, **kwargs):
            return []

        def load_cancelled_uncertain_buy_orders(self, **kwargs):
            return [{**_ORDER, "meta": {}}]

        def apply_ttl_terminal_observation(self, *a, **kw):
            pytest.fail("absence of cancel ACK must retain UNCERTAIN")

    result = reconcile_tqqq_open_buy_ttl(
        repository=Repo(), now=datetime(2026, 10, 20, 15, tzinfo=timezone.utc),
        ttl_seconds=180,
        cancel_order=lambda **kw: pytest.fail("no second cancel"),
        # A broker-confirmed CANCELLED row is itself terminal proof.  Use
        # a nonterminal ACK_PENDING row to test the missing-proof fence.
        query_order=lambda **kw: {**_observation(), "status": "ACK_PENDING"},
    )
    assert result["cancelled_claim_pending"] == 1
    assert result.get("cancelled_claim_recovered", 0) == 0


def test_close_links_unmarked_cancel_odono_only_with_durable_ack_and_original_zero(
    monkeypatch,
):
    from trader.us.db import repos
    from trader.us.execution import order_journal, reconcile
    from trader.us.data_provider import normalize_us_order_status_row
    monkeypatch.setattr(
        repos, "load_pending_ack_orders_result",
        lambda *a, **kw: {"status": "OK", "orders": []},
    )
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [])
    events = [{
        "event_type": "ORDER_CANCELLED",
        "symbol": "TQQQ", "side": "BUY",
        "broker_order_no": "0000038776",
        "client_order_key": "TQQQ_INF_V3:cycle:2026-10-20:BUY",
        "submit_attempt_id": "attempt-A", "qty_requested": 3,
        "meta": {
            "tqqq_ttl_cancel_result": {
                "rt_cd": "0", "msg_cd": "40630000",
                "output": {"ODNO": "0000038961"},
            },
        },
    }]
    monkeypatch.setattr(order_journal, "load_order_events", lambda day: events)

    class Broker:
        def get_today_orders(self, day):
            return [
                normalize_us_order_status_row({
                    "odno": "0000038776", "pdno": "TQQQ", "sll_buy_dvsn_cd": "02",
                    "ord_qty": "3", "ft_ccld_qty": "0", "nccs_qty": "0",
                }),
                # Production KIS may omit RVSE_CNCL_DVSN_CD and ORGN_ODNO.
                normalize_us_order_status_row({
                    "odno": "0000038961", "pdno": "TQQQ", "sll_buy_dvsn_cd": "02",
                    "ord_qty": "3", "ft_ccld_qty": "0", "nccs_qty": "0",
                }),
            ]

    rows = reconcile._load_close_order_attempt_truth(
        provider=Broker(), trade_date="2026-10-20", env="practice"
    )
    assert len(rows) == 1
    assert rows[0]["_broker_cancel_operation_nos"] == ["38961"]
    assert rows[0]["_broker_observation"]["status"] == "CANCELLED"
    assert rows[0]["_broker_observation"]["terminal_proof_source"] == (
        "durable_tqqq_cancel_ack_and_fresh_original_zero_fill_zero_open"
    )

    events[0]["meta"]["tqqq_ttl_cancel_result"]["rt_cd"] = "1"
    rows_no_proof = reconcile._load_close_order_attempt_truth(
        provider=Broker(), trade_date="2026-10-20", env="practice"
    )
    assert len(rows_no_proof) == 2
    assert all("_broker_cancel_operation_nos" not in row for row in rows_no_proof)
