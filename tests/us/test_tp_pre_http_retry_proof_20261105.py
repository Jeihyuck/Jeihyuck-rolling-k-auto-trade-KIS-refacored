"""TP stage retry must be proven pre-HTTP and persist before routing.

These tests never call a broker or create real orders.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from trader.us.market_state_overlay import build_profit_capture_intents


NOW = datetime(2026, 11, 5, 15, 0, tzinfo=timezone.utc)
POSITION = {
    "symbol": "HCA",
    "qty": 20,
    "position_lifecycle_id": "cycle-hca-1",
    "current_price": 110.0,
    "entry_price": 100.0,
}
DATE = "2026-11-05"


def _evaluate(state, *, persist_side_effect=None):
    def _avg(_pos, **_kwargs):
        from decimal import Decimal
        return Decimal("100"), {"broker_avg_price_source": "kis_pchs_avg_pric"}
    with patch("trader.us.profit_capture.authoritative_broker_avg", side_effect=_avg):
        with patch("trader.us.db.repos.mark_us_profit_capture_stage", side_effect=persist_side_effect) as mark:
            out = build_profit_capture_intents(
                [dict(POSITION)], {"profit_capture_enabled": True},
                now=NOW, trade_date=DATE,
                profit_capture_state={"HCA": state},
            )
    return out, mark


def test_first_tp_with_persisted_stage_routes_once():
    out, mark = _evaluate({})
    assert len(out) == 1
    assert out[0]["reason"] == "TAKE_PROFIT_TP1"
    assert out[0]["client_order_key"].endswith("_TAKE_PROFIT_TP1")
    mark.assert_called_once()


def test_db_stage_write_failure_does_not_route_broker_intent():
    out, mark = _evaluate({}, persist_side_effect=RuntimeError("db down"))
    assert out == []
    mark.assert_called_once()


def test_old_unknown_attempt_does_not_create_new_r_key():
    state = {
        "meta": {
            "tp1_order_keys": ["old_attempt"],
            "last_status": "FAILED",
            "last_evidence_type": "UNVERIFIED",
        },
    }
    out, mark = _evaluate(state)
    assert out == []
    mark.assert_not_called()


def test_proven_pre_http_abort_can_create_new_attempt():
    state = {
        "meta": {
            "tp1_order_keys": ["old_attempt"],
            "last_status": "FAILED",
            "last_evidence_type": "BROKER_SUBMIT_ABORTED_PRE_IO",
        },
    }
    out, mark = _evaluate(state)
    assert len(out) == 1
    assert out[0]["client_order_key"].endswith("_R2")
    mark.assert_called_once()


def test_ack_with_order_number_blocks_pre_http_retry_even_if_failed_marker():
    state = {
        "tp1_broker_order_no": "0000012345",
        "meta": {
            "tp1_order_keys": ["old_attempt"],
            "last_status": "FAILED",
            "last_evidence_type": "BROKER_SUBMIT_ABORTED_PRE_IO",
        },
    }
    out, mark = _evaluate(state)
    assert out == []
    mark.assert_not_called()
