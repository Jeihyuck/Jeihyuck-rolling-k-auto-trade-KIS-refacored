from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from trader.pb1_runtime_guards import _apply_buyable_candidate_backfill
from trader.reconcile_kis import _promote_open_buy_orders_from_holdings


class _Engine:
    _buyable_gate_context = {}


def test_incident_scenario_a_no_authoritative_buyable_candidate() -> None:
    engine = _Engine()
    result = SimpleNamespace(orderable_candidates=[], backfill_attempted=False, backfill_added_count=0)
    added = _apply_buyable_candidate_backfill(
        engine,
        result=result,
        orderable_candidates=result.orderable_candidates,
        candidates=[SimpleNamespace(code="090430", features={"score_final": 10.0}, planned_qty=1)],
        new_position_limit=3,
        target_new_positions=3,
        tick_budget_krw=1_000_000,
        planned_spent=0,
        available_cash_krw=1_000_000,
        min_order_krw=0,
    )
    assert added == 0
    assert result.orderable_candidates == []


def test_incident_scenario_c_existing_holding_without_delta_stays_acked() -> None:
    orders_repo = MagicMock()
    fills_repo = MagicMock()
    orders_repo.get_open_orders.return_value = [
        {
            "order_id": "ord-1",
            "code": "090430",
            "side": "BUY",
            "status": "ACKED",
            "qty": 1,
            "request_json": {"pre_order_holding_qty": 1, "requested_qty": 1, "submitted_qty": 1},
            "response_json": {"_order_execution": {"requested_qty": 1, "submitted_qty": 1}},
            "client_order_key": "k",
            "kis_odno": "o",
            "submitted_at": datetime(2026, 8, 4, 13, 0, 0),
            "acked_at": datetime(2026, 8, 4, 13, 0, 1),
        }
    ]
    result = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="best_k_meta",
        ctx_run_id="run-1",
        tick_ts=datetime(2026, 8, 4, 13, 1, 0),
        holdings_rows=[{"pdno": "090430", "hldg_qty": "1", "pchs_avg_pric": "10000"}],
        orders_repo=orders_repo,
        fills_repo=fills_repo,
    )
    assert result["fills"] == 0
    fills_repo.upsert_fill.assert_not_called()
    assert orders_repo.upsert_reconciled_order.call_args.kwargs["status"] == "ACKED"
