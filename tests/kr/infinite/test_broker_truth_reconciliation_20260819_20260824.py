from __future__ import annotations

from datetime import date

from trader.kr.infinite.executor import KISExecutor
from trader.kr.infinite.models import Action, Decision, OrderIntent, State
from trader.kr.infinite.repository import InfiniteRepository


class _Ccld500:
    def inquire_daily_ccld(self, **kwargs):
        raise RuntimeError("HTTP 500")


class _CcldEmpty:
    def inquire_daily_ccld(self, **kwargs):
        return {"rt_cd": "0", "output1": []}


def _balance(qty: int, avg: float) -> dict:
    return {
        "output1": [{
            "pdno": "122630",
            "hldg_qty": str(qty),
            "ord_psbl_qty": str(qty),
            "pchs_avg_pric": str(avg),
        }],
        "output2": [{"tot_evlu_amt": "10000000"}],
    }


def test_20260819_new_cycle_buy_persists_flat_baseline() -> None:
    state = State(cycle_id="KRINF-20260819-test", cycle_start_date=date(2026, 8, 19))
    decision = Decision(
        Action.BUY,
        "NEW_CYCLE_BUY",
        qty=6,
        notional=663000.0,
        idempotency_key="buy-0819",
    )
    meta = InfiniteRepository._intent_metadata(state, decision)
    assert meta["strategy_owner"] == "KR_INFINITE"
    assert meta["pre_order_holding_qty"] == 0
    assert meta["pre_order_avg_price"] == 0.0


def test_20260824_tp_sell_persists_six_share_baseline() -> None:
    state = State(cycle_id="KRINF-20260819-test", cycle_start_date=date(2026, 8, 19))
    decision = Decision(
        Action.SELL_PARTIAL,
        "TAKE_PROFIT_TP1",
        qty=3,
        notional=390000.0,
        idempotency_key="sell-0824",
        metadata={"remaining_qty": 3, "desired_profit_stage": "TP1"},
    )
    meta = InfiniteRepository._intent_metadata(state, decision)
    assert meta["pre_order_holding_qty"] == 6


def test_20260824_ccld_http500_sell_uses_balance_delta_6_to_3() -> None:
    intent = OrderIntent(
        id=2,
        cycle_id="KRINF-20260819-test",
        trade_date=date(2026, 8, 24),
        side="SELL_PARTIAL",
        idempotency_key="sell-0824",
        requested_qty=3,
        broker_order_id="0000000058",
        status="SUBMITTED",
        metadata={"pre_order_holding_qty": 6, "reconcile_contract_version": "KR_INF_BALANCE_DELTA_V1"},
    )
    executor = KISExecutor(_Ccld500(), "practice", balance_snapshot=_balance(3, 110500.0))
    broker = executor.order_state(intent, date(2026, 8, 24))
    assert broker.status == "FILLED"
    assert broker.filled_qty == 3


def test_20260819_ccld_failure_buy_uses_balance_delta_0_to_6() -> None:
    intent = OrderIntent(
        id=1,
        cycle_id="KRINF-20260819-test",
        trade_date=date(2026, 8, 19),
        side="BUY",
        idempotency_key="buy-0819",
        requested_qty=6,
        broker_order_id="0000000060",
        status="SUBMITTED",
        metadata={"pre_order_holding_qty": 0, "pre_order_avg_price": 0.0},
    )
    executor = KISExecutor(_Ccld500(), "practice", balance_snapshot=_balance(6, 110500.0))
    broker = executor.order_state(intent, date(2026, 8, 19))
    assert broker.status == "FILLED"
    assert broker.filled_qty == 6
    assert broker.filled_avg_price == 110500.0
    assert broker.filled_notional_krw == 663000.0


def test_historical_zero_fill_day_order_expires_after_successful_lookup() -> None:
    intent = OrderIntent(
        id=3,
        cycle_id="KRINF-test",
        trade_date=date(2026, 8, 24),
        side="SELL_PARTIAL",
        idempotency_key="sell-expired",
        requested_qty=3,
        broker_order_id="0000000999",
        status="SUBMITTED",
        metadata={"pre_order_holding_qty": 3},
    )
    executor = KISExecutor(_CcldEmpty(), "practice", balance_snapshot=_balance(3, 110500.0))
    broker = executor.order_state(intent, date(2026, 9, 11))
    assert broker.status == "EXPIRED"
    assert broker.filled_qty == 0


def test_legacy_no_baseline_and_failed_lookup_stays_fenced_not_guessed() -> None:
    intent = OrderIntent(
        id=4,
        cycle_id="KRINF-legacy",
        trade_date=date(2026, 8, 24),
        side="SELL_ALL",
        idempotency_key="legacy",
        requested_qty=3,
        broker_order_id="0000000998",
        status="RECONCILE_PENDING",
        metadata={},
    )
    executor = KISExecutor(_Ccld500(), "practice", balance_snapshot=_balance(3, 110500.0))
    broker = executor.order_state(intent, date(2026, 9, 11))
    assert broker.status == "RECONCILE_PENDING"
