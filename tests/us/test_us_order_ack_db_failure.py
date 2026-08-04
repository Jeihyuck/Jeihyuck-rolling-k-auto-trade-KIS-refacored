# -*- coding: utf-8 -*-
"""Phase 9: KIS ACK 성공 후 DB ACK 실패 시 REJECT 아닌 ACK_DB_FAILED 반환 검증."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


def _make_intent(symbol: str = "AAPL", side: str = "BUY") -> dict:
    return {
        "symbol": symbol,
        "exchange": "NASDAQ",
        "side": side,
        "qty": 5,
        "limit_price": 150.0,
        "notional_usd": 750.0,
        "client_order_key": f"test_{symbol}_{side}",
    }


def _make_kis_client(order_no: str = "ORDER123") -> MagicMock:
    client = MagicMock()
    client.place_us_buy_order.return_value = {"rt_cd": "0", "output": {"ODNO": order_no}}
    client.place_us_sell_order.return_value = {"rt_cd": "0", "output": {"ODNO": order_no}}
    return client


def _patch_risk_gate():
    """order_router에서 import된 assert_order_allowed를 mock."""
    return patch("trader.us.execution.order_router.assert_order_allowed", return_value=None)


def test_ack_db_failure_returns_ack_db_failed():
    """KIS 성공 + DB save_order_ack 실패 → status=ACK_DB_FAILED, REJECT 아님."""
    from trader.us.execution.order_router import route_order

    intent = _make_intent()
    kis_client = _make_kis_client()

    with (
        _patch_risk_gate(),
        patch("trader.us.execution.order_router.resolve_dry_run_for_us_order", return_value=False),
        patch("trader.us.db.repos.save_order_intent", return_value=True),
        patch("trader.us.db.repos.load_today_order_keys", return_value=set()),
        patch("trader.us.execution.kis_us_response_parser.extract_order_no", return_value="ORDER123"),
        patch("trader.us.db.repos.save_order_ack", side_effect=RuntimeError("DB down")),
        patch("trader.us.db.repos.save_order_reject") as mock_reject,
        patch("trader.us.db.repos.mark_order_intent_sent", return_value=None),
        patch("trader.us.db.repos.mark_order_intent_rejected") as mock_intent_reject,
    ):
        result = route_order(
            intent,
            kis_client=kis_client,
            kis_order_allowed=True,
        )

    assert result["status"] == "ACK_DB_FAILED", (
        f"KIS 성공 후 DB 실패는 ACK_DB_FAILED여야 함: status={result['status']}"
    )
    assert result.get("kis_ack") is True
    assert result.get("ack_db_saved") is False
    assert result.get("requires_reconcile") is True
    mock_reject.assert_not_called()
    mock_intent_reject.assert_not_called()


def test_ack_db_success_returns_ack():
    """KIS 성공 + DB 저장 성공 → status=ACK."""
    from trader.us.execution.order_router import route_order

    intent = _make_intent()
    kis_client = _make_kis_client()

    with (
        _patch_risk_gate(),
        patch("trader.us.execution.order_router.resolve_dry_run_for_us_order", return_value=False),
        patch("trader.us.db.repos.save_order_intent", return_value=True),
        patch("trader.us.db.repos.load_today_order_keys", return_value=set()),
        patch("trader.us.execution.kis_us_response_parser.extract_order_no", return_value="ORDER123"),
        patch("trader.us.db.repos.save_order_ack", return_value=True),
        patch("trader.us.db.repos.mark_order_intent_sent", return_value=None),
    ):
        result = route_order(
            intent,
            kis_client=kis_client,
            kis_order_allowed=True,
        )

    assert result["status"] == "ACK"
    assert result.get("kis_ack") is True
    assert result.get("ack_db_saved") is True
    assert result.get("requires_reconcile") is False


def test_kis_failure_returns_reject():
    """KIS API 자체 실패 → status=REJECT."""
    from trader.us.execution.order_router import route_order

    intent = _make_intent()
    kis_client = MagicMock()
    kis_client.place_us_buy_order.side_effect = RuntimeError("KIS 업무 거절: 주문 불가")

    with (
        _patch_risk_gate(),
        patch("trader.us.execution.order_router.resolve_dry_run_for_us_order", return_value=False),
        patch("trader.us.db.repos.save_order_intent", return_value=True),
        patch("trader.us.db.repos.load_today_order_keys", return_value=set()),
        patch("trader.us.db.repos.save_order_reject", return_value=True) as mock_reject,
        patch("trader.us.db.repos.mark_order_intent_rejected", return_value=None),
    ):
        result = route_order(
            intent,
            kis_client=kis_client,
            kis_order_allowed=True,
        )

    assert result["status"] == "REJECT"
    assert result.get("kis_ack") is False
    mock_reject.assert_called_once()


def test_mark_intent_sent_failure_does_not_change_status():
    """mark_order_intent_sent 실패해도 status=ACK 유지."""
    from trader.us.execution.order_router import route_order

    intent = _make_intent()
    kis_client = _make_kis_client()

    with (
        _patch_risk_gate(),
        patch("trader.us.execution.order_router.resolve_dry_run_for_us_order", return_value=False),
        patch("trader.us.db.repos.save_order_intent", return_value=True),
        patch("trader.us.db.repos.load_today_order_keys", return_value=set()),
        patch("trader.us.execution.kis_us_response_parser.extract_order_no", return_value="ORDER123"),
        patch("trader.us.db.repos.save_order_ack", return_value=True),
        patch("trader.us.db.repos.mark_order_intent_sent", side_effect=RuntimeError("DB intent fail")),
    ):
        result = route_order(
            intent,
            kis_client=kis_client,
            kis_order_allowed=True,
        )

    assert result["status"] == "ACK"
    assert result.get("kis_ack") is True
    assert result.get("ack_db_saved") is True
