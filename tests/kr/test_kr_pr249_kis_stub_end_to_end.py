"""PR249 isolated E2E: KIS stub → exact order → fill → position economics.

No KIS order submission, live credentials, Supabase network, or production DB.
Exercises reconcile_today with the real SQLite repositories; the old order
remains broker-ACKED and KIS returns repeated cumulative order snapshots.
"""
from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.reconcile_kis import reconcile_today
from trader.run_context import RunContext
from tests.kr.test_kr_20261006_execution_convergence import _db, _open_position


class KISDailyReplay:
    def __init__(self, row):
        self.row = dict(row)
        self.calls = 0

    def inquire_daily_ccld(self, **kwargs):
        self.calls += 1
        assert kwargs["start_date"] == "20261022"
        assert kwargs["end_date"] == "20261022"
        return {"rt_cd": "0", "output1": [dict(self.row)], "output2": []}

    def __getattr__(self, key):
        if any(tag in key.lower() for tag in ("order", "cancel", "submit", "sell", "buy")):
            raise AssertionError("KIS broker order/cancel path must never be called")
        raise AttributeError(key)


def _accepted_sell(engine, *, code, ordered_qty, pre_qty, avg_price, odno, order_key):
    pos = _open_position(engine, code=code, qty=pre_qty, avg=avg_price)
    repo = OrdersRepo(engine)
    order_id, created = repo.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code=code, market="J", side="SELL",
        ord_type="MARKET", qty=ordered_qty, limit_price=avg_price,
        stage="PB1-CLOSE", client_order_key=order_key,
        request_json={
            "pre_order_holding_qty": pre_qty,
            "requested_qty": ordered_qty,
            "submitted_qty": ordered_qty,
            "position_lifecycle_id": str(pos["position_cycle_id"]),
            "strategy_owner": "KR_STANDARD",
            "semantic_action": "SELL:EXIT_DAY:FULL_EXIT",
        },
        status="ACKED", account_id=get_account_key(env="practice"),
        portfolio_epoch_id=str(pos["portfolio_epoch_id"]),
        position_cycle_id=str(pos["position_cycle_id"]),
    )
    assert created
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        conn.execute(sa.update(schema.orders).where(
            schema.orders.c.order_id == order_id
        ).values(
            kis_odno=odno, created_at=datetime(2026, 10, 22, 15, 18,
                                             tzinfo=ZoneInfo("Asia/Seoul")),
        ))
    return pos, str(order_id)


@pytest.mark.parametrize("code,initial_qty,avg,request_qty,cumulative,notional,odno", [
    ("251970", 17, 68400.0, 17, 10, 685000.0, "0000040226"),
    ("253590", 79, 16130.0, 79, 52, 836180.0, "0000040083"),
])
def test_kis_daily_stub_reconcile_twice_preserves_partial_and_exact_cycle(
    monkeypatch, code, initial_qty, avg, request_qty, cumulative, notional, odno,
):
    import trader.reconcile_kis as rk
    engine = _db()
    pos, order_id = _accepted_sell(
        engine, code=code, ordered_qty=request_qty, pre_qty=initial_qty,
        avg_price=avg, odno=odno, order_key="pr249-e2e-" + code,
    )
    row = {
        "pdno": code, "sll_buy_dvsn_cd": "01",
        "ord_qty": str(request_qty), "tot_ccld_qty": str(cumulative),
        "tot_ccld_amt": str(notional), "avg_prvs": str(notional / cumulative),
        "odno": odno, "ord_dt": "20261022", "ord_tmd": "151810",
        "ord_stat_cd": "PARTIAL_FILLED", "market": "J",
    }
    kis = KISDailyReplay(row)
    ctx = RunContext(
        run_id=None, env="practice", strategy="pb1_pullback_close",
        started_at=datetime(2026, 10, 22, 15, 18, tzinfo=ZoneInfo("Asia/Seoul")),
        dry_run=True,
    )
    monkeypatch.setattr(rk, "_execution_claim_integrity_reason", lambda *a, **k: None)
    # Source order and full database repositories are real. No safety guard
    # is bypassed on the order/position/fill economic writing path.
    for _ in range(2):
        reconcile_today(
            engine=engine, kis=kis, ctx=ctx, trade_date=date(2026, 10, 22),
        )
    # Late cancellation acknowledges the 27 (or 7) unfilled remainder,
    # but it cannot erase the prior partial execution or resubmit a SELL.
    kis.row["ord_stat_cd"] = "CANCELLED"
    reconcile_today(
        engine=engine, kis=kis, ctx=ctx, trade_date=date(2026, 10, 22),
    )
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        fills = conn.execute(sa.select(schema.fills).where(sa.and_(
            schema.fills.c.order_id == order_id,
            schema.fills.c.side == "SELL",
        ))).mappings().all()
        position = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == str(pos["position_cycle_id"])
        )).mappings().one()
    assert kis.calls == 3
    assert len(fills) == 1
    assert fills[0]["qty"] == cumulative
    assert fills[0]["price"] == pytest.approx(notional / cumulative)
    assert str(fills[0]["position_cycle_id"]) == str(pos["position_cycle_id"])
    assert position["qty"] == initial_qty - cumulative
    assert position["status"] == "OPEN"
    assert float(position["realized_pnl"]) == pytest.approx(notional - avg*cumulative)
    assert position["position_meta"]["realized_pnl_status"] == "CONFIRMED"
