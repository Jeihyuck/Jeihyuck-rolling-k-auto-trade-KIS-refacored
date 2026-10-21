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
