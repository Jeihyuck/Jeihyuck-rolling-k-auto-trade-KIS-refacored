"""2026-11-03 KR historical SELL: provisional manual MTS to original KIS proof.

Synthetic SQLite only. No KIS HTTP, real broker order, or production DB write.
"""
from datetime import datetime, timezone

import pytest
import sqlalchemy as sa

from trader.db.repos import PositionsRepo
from trader.db.schema import schema_for_engine
from tests.kr.test_kr_20261006_execution_convergence import _db, _open_position


@pytest.mark.parametrize("code,qty,avg,old_notional,kis_notional,old_pnl", [
    ("293490", 170, 11510.35294117647, 1918960, 1919100, -37800),
    ("425420", 30, 62996.666666666664, 1868190, 1871200, -21710),
])
def test_provisional_sell_reprices_only_with_exact_kis_original_order_proof(
    code, qty, avg, old_notional, kis_notional, old_pnl
):
    engine = _db()
    repo = PositionsRepo(engine)
    pos = _open_position(engine, code=code, qty=qty, avg=avg)
    schema = schema_for_engine(engine)
    cycle, epoch = str(pos["position_cycle_id"]), str(pos["portfolio_epoch_id"])
    oid = f"historic-original-kis-{code}"
    marker = {
        "fill_price": old_notional / qty,
        "qty_accounted": qty,
        "pnl_accounted_qty": qty,
        "broker_holding_qty": 0,
        "cost_basis_at_sell": avg,
        "realized_pnl_status": "MANUAL_MTS_PRICE_PROVISIONAL",
        "pnl_accounted_notional": old_notional,
        "confirmed_cumulative_qty": 17 if code == "293490" else 0,
        "manual_mts_price_provisional": True,
        "manual_mts_source": "USER_MTS_DISPLAY_AVG_20261026",
    }
    before_closed_ts = datetime(2026, 10, 26, 6, 16, tzinfo=timezone.utc)
    with engine.begin() as conn:
        conn.execute(sa.update(schema.positions).where(
            schema.positions.c.position_id == pos["position_id"]
        ).values(
            qty=0, total_cost=0, avg_buy_price=avg,
            status="CLOSED", closed_reason="BROKER_ZERO_ECONOMIC_SETTLEMENT_PENDING",
            closed_ts=before_closed_ts, realized_pnl=old_pnl,
            position_meta={"sell_execution_reconcile": {oid: marker}},
        ))
    kwargs = dict(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code=code, market="J",
        confirmed_cumulative_qty=qty, fill_price=kis_notional / qty,
        filled_at=datetime(2026, 11, 3, 6, 16, tzinfo=timezone.utc),
        position_cycle_id=cycle, portfolio_epoch_id=epoch,
        order_id=oid, pre_order_holding_qty=None, broker_holding_qty=None,
    )
    with pytest.raises(RuntimeError, match="KR_SELL_RECONCILE_CONFIRMED_PRICE_CONFLICT"):
        repo.reconcile_sell_execution(**kwargs)
    with engine.connect() as conn:
        pre = dict(conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_id == pos["position_id"]
        )).mappings().one())
    assert pre["realized_pnl"] == pytest.approx(old_pnl)

    first = repo.reconcile_sell_execution(
        **kwargs, native_original_order_price_proof=True,
    )
    second = repo.reconcile_sell_execution(
        **kwargs, native_original_order_price_proof=True,
    )
    assert first["pnl_qty_applied"] == second["pnl_qty_applied"] == 0
    with engine.connect() as conn:
        after = dict(conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_id == pos["position_id"]
        )).mappings().one())
    assert after["qty"] == 0 and after["status"] == "CLOSED"
    assert after["closed_reason"] == "FULL_SELL"
    assert after["closed_ts"] == pre["closed_ts"]
    assert after["total_cost"] == 0
    assert after["realized_pnl"] == pytest.approx(old_pnl + kis_notional - old_notional)
    state = after["position_meta"]["sell_execution_reconcile"][oid]
    assert state["realized_pnl_status"] == "CONFIRMED"
    assert state["manual_mts_price_provisional"] is False
    assert state["manual_mts_original_notional"] == old_notional
    assert state["pnl_accounted_notional"] == pytest.approx(kis_notional)
    assert state["realized_pnl_net_status"] == "FEES_TAX_UNVERIFIED"

    with pytest.raises(RuntimeError, match="KR_SELL_RECONCILE_CONFIRMED_PRICE_CONFLICT"):
        repo.reconcile_sell_execution(
            **{**kwargs, "fill_price": (kis_notional + 5) / qty},
            native_original_order_price_proof=True,
        )
