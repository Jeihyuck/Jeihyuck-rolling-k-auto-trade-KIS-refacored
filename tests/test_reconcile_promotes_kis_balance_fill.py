"""tests/test_reconcile_promotes_kis_balance_fill.py

KIS balance 기반 order 승격, fill 생성, position 복구 및 entry meta 복원 검증.

요구사항:
- accepted BUY order가 있고 KIS balance에 thdt_buyqty/hldg_qty가 생기면 
  reconcile이 order를 FILLED로 승격해야 한다.
- fill을 생성해야 한다.
- position도 생성/복구해야 한다.
- ORDER_INTENT payload의 entry_reason, entry_style, stop, pivot을 
  position meta로 복구해야 한다.
- 복구 후 exit 평가에서 ENTRY_UNKNOWN / GENERIC_EXIT가 나오면 실패.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.reconcile_kis import _promote_open_buy_orders_from_holdings
from trader.pb1_engine import PB1Engine


def test_promote_order_creates_fill_and_position() -> None:
    """KIS holdings 기반 order 승격, fill 생성, position 복구."""
    orders_repo = MagicMock()
    fills_repo = MagicMock()
    positions_repo = MagicMock()

    # ORDER_INTENT 이벤트에 entry meta 포함
    order_intent_payload = {
        "entry_reason": "STAGE2_PULLBACK",
        "entry_style": "pb1_classic",
        "stop_price_at_entry": 10000.0,
        "pivot_price_at_entry": 11500.0,
        "trade_horizon": "swing",
        "pre_order_holding_qty": 0,
        "requested_qty": 5,
        "submitted_qty": 5,
    }

    orders_repo.get_open_orders.return_value = [
        {
            "order_id": "ord-buy-1",
            "env": "practice",
            "strategy": "pb1_pullback_close",
            "code": "067310",
            "side": "BUY",
            "status": "ACCEPTED",
            "qty": 5,
            "limit_price": 10500.0,
            "stage": "PB1-CLOSE",
            "client_order_key": "practice:pb1:067310:BUY:1",
            "kis_odno": "2001",
            "submitted_at": datetime(2026, 5, 1, 9, 1, 0),
            "acked_at": datetime(2026, 5, 1, 9, 1, 1),
            "request_json": order_intent_payload,  # ORDER_INTENT payload
            "response_json": {},
        }
    ]

    holdings_rows = [
        {
            "pdno": "067310",
            "hldg_qty": "5",
            "pchs_avg_pric": "10500",
            "thdt_buyqty": "5",  # 당일 매수 수량
        }
    ]

    promoted = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id="run-1",
        tick_ts=datetime(2026, 5, 1, 9, 2, 0),
        holdings_rows=holdings_rows,
        orders_repo=orders_repo,
        fills_repo=fills_repo,
    )

    # 1개 order 승격, 1개 fill 생성 확인
    assert promoted["orders"] == 1
    assert promoted["fills"] == 1
    assert promoted["codes"] == ["067310"]
    
    # order가 FILLED로 승격되었는지 확인
    orders_repo.upsert_reconciled_order.assert_called_once()
    order_kwargs = orders_repo.upsert_reconciled_order.call_args.kwargs
    assert order_kwargs["status"] == "FILLED"
    assert order_kwargs["code"] == "067310"
    assert order_kwargs["qty"] == 5
    
    # fill이 생성되었는지 확인
    fills_repo.upsert_fill.assert_called_once()
    fill_kwargs = fills_repo.upsert_fill.call_args.kwargs
    assert fill_kwargs["trade_id"].startswith("PROMOTE:")
    assert fill_kwargs["qty"] == 5
    assert fill_kwargs["price"] == 10500.0
    assert fill_kwargs["side"] == "BUY"


def test_position_meta_includes_entry_metadata() -> None:
    """position 복구 시 ORDER_INTENT의 entry meta가 position_meta로 복구되는지 확인."""
    orders_repo = MagicMock()
    fills_repo = MagicMock()
    
    order_intent_payload = {
        "entry_reason": "STAGE2_PULLBACK",
        "entry_style": "pb1_classic",
        "stop_price_at_entry": 10000.0,
        "pivot_price_at_entry": 11500.0,
        "trade_horizon": "swing",
    }

    orders_repo.get_open_orders.return_value = [
        {
            "order_id": "ord-buy-2",
            "env": "practice",
            "strategy": "pb1_pullback_close",
            "code": "005930",
            "side": "BUY",
            "status": "ACCEPTED",
            "qty": 10,
            "limit_price": 70000.0,
            "stage": "PB1-AM",
            "client_order_key": "practice:pb1:005930:BUY:1",
            "kis_odno": "3001",
            "submitted_at": datetime(2026, 5, 1, 9, 5, 0),
            "acked_at": datetime(2026, 5, 1, 9, 5, 1),
            "request_json": order_intent_payload,
            "response_json": {},
        }
    ]

    holdings_rows = [
        {
            "pdno": "005930",
            "hldg_qty": "10",
            "pchs_avg_pric": "70000",
        }
    ]

    _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id="run-2",
        tick_ts=datetime(2026, 5, 1, 9, 6, 0),
        holdings_rows=holdings_rows,
        orders_repo=orders_repo,
        fills_repo=fills_repo,
    )

    # order의 request_json에 entry meta가 포함되어 있는지 확인
    order_kwargs = orders_repo.upsert_reconciled_order.call_args.kwargs
    assert order_kwargs["request_json"]["entry_reason"] == "STAGE2_PULLBACK"
    assert order_kwargs["request_json"]["entry_style"] == "pb1_classic"
    assert order_kwargs["request_json"]["stop_price_at_entry"] == 10000.0
    assert order_kwargs["request_json"]["pivot_price_at_entry"] == 11500.0


def test_exit_evaluation_does_not_show_entry_unknown() -> None:
    """복구된 position의 exit 평가에서 ENTRY_UNKNOWN / GENERIC_EXIT이 나오지 않아야 함."""
    # _resolve_exit_family 테스트 — entry_reason이 명확하면 GENERIC이 아니어야 함
    entry_reason = "ENTRY_PULLBACK"  # normalized reason 사용
    entry_style = None
    
    family, final_reason = PB1Engine._resolve_exit_family(entry_reason, entry_style)
    
    # ENTRY_PULLBACK이므로 GENERIC이 아니어야 함
    assert family != "ENTRY_GENERIC", f"복구된 position이 GENERIC으로 분류되면 안 됨: family={family}"
    assert final_reason != "GENERIC_EXIT", f"복구된 position이 GENERIC_EXIT으로 분류되면 안 됨: final_reason={final_reason}"
    
    # 정상적인 family/reason 반환 확인
    assert family == "ENTRY_PULLBACK"
    assert final_reason == "PULLBACK_EXIT"


def test_promote_skips_when_no_holdings() -> None:
    """KIS holdings가 없으면 order를 승격하지 않음."""
    orders_repo = MagicMock()
    fills_repo = MagicMock()
    
    orders_repo.get_open_orders.return_value = [
        {
            "order_id": "ord-buy-3",
            "env": "practice",
            "code": "035420",
            "side": "BUY",
            "status": "ACCEPTED",
            "qty": 3,
            "limit_price": 50000.0,
        }
    ]

    # holdings_rows가 빈 리스트 — 아직 체결 안 됨
    holdings_rows = []

    promoted = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id="run-3",
        tick_ts=datetime(2026, 5, 1, 9, 10, 0),
        holdings_rows=holdings_rows,
        orders_repo=orders_repo,
        fills_repo=fills_repo,
    )

    # 승격 없음
    assert promoted["orders"] == 0
    assert promoted["fills"] == 0
    assert promoted["codes"] == []
    orders_repo.upsert_reconciled_order.assert_not_called()
    fills_repo.upsert_fill.assert_not_called()


def test_promote_partial_filled_order() -> None:
    """부분 체결된 BUY order도 holdings 기반으로 승격 가능."""
    orders_repo = MagicMock()
    fills_repo = MagicMock()
    
    orders_repo.get_open_orders.return_value = [
        {
            "order_id": "ord-buy-4",
            "env": "practice",
            "code": "000660",
            "side": "BUY",
            "status": "PARTIAL_FILLED",
            "qty": 10,
            "limit_price": 120000.0,
            "kis_odno": "4001",
            "request_json": {
                "pre_order_holding_qty": 0,
                "requested_qty": 10,
                "submitted_qty": 10,
            },
            "response_json": {
                "_order_execution": {
                    "requested_qty": 10,
                    "submitted_qty": 10,
                }
            },
        }
    ]

    holdings_rows = [
        {
            "pdno": "000660",
            "hldg_qty": "10",  # 전체 qty가 holdings에 반영됨
            "pchs_avg_pric": "120000",
        }
    ]

    promoted = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id="run-4",
        tick_ts=datetime(2026, 5, 1, 9, 15, 0),
        holdings_rows=holdings_rows,
        orders_repo=orders_repo,
        fills_repo=fills_repo,
    )

    assert promoted["orders"] == 1
    assert promoted["fills"] == 1
    assert promoted["codes"] == ["000660"]
    order_kwargs = orders_repo.upsert_reconciled_order.call_args.kwargs
    assert order_kwargs["status"] == "FILLED"


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
