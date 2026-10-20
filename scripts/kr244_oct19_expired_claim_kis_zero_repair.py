"""One-time practice Oct19 PB1 EXPIRED/CREATED claim reconciliation.

Requires authoritative complete KIS ALL-exchange daily CCLD = ZERO for the
entire account/day, original CREATED attempt, EXPIRED local order and no broker
submit/ack/ID/DB fills. Never creates, cancels or changes a broker order.
"""
from __future__ import annotations
from datetime import date, datetime, timezone
import hashlib
import json
import os
import sqlalchemy as sa


def prove_broker_zero_day(response: dict) -> bool:
    return bool(
        isinstance(response, dict)
        and str(response.get("rt_cd") or "") == "0"
        and response.get("_sep29_complete") is True
        and not response.get("_diag_stub")
        and not response.get("_ccld_status")
        and isinstance(response.get("output1"), list)
        and len(response["output1"]) == 0
    )


def run() -> dict:
    if (
        os.getenv("KIS_ENV","").lower()!="practice"
        or os.getenv("STRATEGY_ENV","").lower()!="practice"
        or os.getenv("DISABLE_REAL_TRADING")!="1"
        or os.getenv("LIVE_TRADING_ENABLED")!="0"
        or os.getenv("NO_TRADE")!="1"
        or "openapivts.koreainvestment.com" not in str(os.getenv("KIS_REST_URL") or "")
    ):
        raise RuntimeError("PRACTICE_NO_ORDER_GATES_REQUIRED")
    from trader.account_state import get_account_key
    account=get_account_key(env="practice")
    digest=hashlib.sha256(account.encode()).hexdigest()
    if not digest.startswith("fef5d08b17"):
        raise RuntimeError("ORIGINAL_ACCOUNT_IDENTITY_MISMATCH")
    from trader.kis_wrapper import KisAPI
    from trader.kr.runtime_integrity_20260929 import _daily_ccld_all_pages_for_date
    day=date(2026,10,19)
    r=_daily_ccld_all_pages_for_date(KisAPI(kis_env="practice"),day)
    if not prove_broker_zero_day(r):
        raise RuntimeError("KIS_ZERO_ORDER_DAY_NOT_AUTHORITATIVELY_PROVEN")

    from trader.db.engine import get_engine
    from trader.db.schema import schema_for_engine
    from trader.db.repos import OrdersRepo
    engine=get_engine()
    schema=schema_for_engine(engine)
    actions, attempts, orders=schema.execution_claims,schema.execution_attempts,schema.orders
    with engine.connect() as conn:
        valid=conn.execute(sa.select(
            actions.c.action_key,actions.c.active_attempt_id,
            attempts.c.attempt_state,attempts.c.authoritative,orders.c.order_id
        ).select_from(
            actions.join(attempts,sa.and_(
                actions.c.action_key==attempts.c.action_key,
                actions.c.active_attempt_id==attempts.c.attempt_id
            )).join(orders,sa.and_(
                orders.c.env==actions.c.env,
                orders.c.trading_epoch_id==actions.c.trading_epoch_id,
                orders.c.client_order_key==attempts.c.client_order_key
            ))
        ).where(
            actions.c.env=="practice",actions.c.account_id==digest,
            actions.c.market=="KR",actions.c.strategy_owner=="PB1",
            actions.c.trade_date==day,actions.c.action=="BUY_ENTRY:PB1-AM",
            actions.c.action_state=="IN_FLIGHT",
            orders.c.code=="192820",orders.c.side=="BUY",
            orders.c.strategy=="pb1_pullback_close",
            orders.c.status=="EXPIRED",
            orders.c.kis_odno.is_(None),orders.c.broker_order_id.is_(None),
            orders.c.submitted_at.is_(None),orders.c.acked_at.is_(None),
            attempts.c.attempt_state=="CREATED",
            attempts.c.authoritative.is_(False),
        )).mappings().all()
        original_accepted=conn.execute(
            sa.select(sa.func.count()).select_from(orders).where(
                orders.c.env=="practice",
                orders.c.created_at >= datetime(2026,10,18,15,tzinfo=timezone.utc),
                orders.c.created_at < datetime(2026,10,19,15,tzinfo=timezone.utc),
                sa.or_(orders.c.kis_odno.is_not(None),
                    orders.c.submitted_at.is_not(None),
                    orders.c.acked_at.is_not(None)),
            )
        ).scalar_one()
        original_fills=conn.execute(
            sa.select(sa.func.count()).select_from(schema.fills).where(
                schema.fills.c.env=="practice",
                schema.fills.c.filled_at >= datetime(2026,10,18,15,tzinfo=timezone.utc),
                schema.fills.c.filled_at < datetime(2026,10,19,15,tzinfo=timezone.utc),
            )
        ).scalar_one()
        target_fills=conn.execute(
            sa.select(sa.func.count()).select_from(schema.fills).where(
                schema.fills.c.env=="practice",schema.fills.c.code=="192820",
                schema.fills.c.filled_at >= datetime(2026,10,18,15,tzinfo=timezone.utc),
                schema.fills.c.filled_at < datetime(2026,10,19,15,tzinfo=timezone.utc),
            )
        ).scalar_one()
    if len(valid)!=1 or original_accepted or original_fills or target_fills:
        raise RuntimeError("HISTORICAL_BROKER_ZERO_DB_ATTEMPT_PROOF_MISMATCH")
    row=valid[0]
    # No speculative live order, no broker ID to cancel: release the old
    # local fence after proving the entire day's broker order history is empty.
    OrdersRepo(engine)._execution_claim_repo.release_before_submit(
        str(row["action_key"]),attempt_id=str(row["active_attempt_id"]),
    )
    with engine.connect() as conn:
        state=conn.execute(
            sa.select(actions.c.action_state,actions.c.active_attempt_id)
            .where(actions.c.action_key==str(row["action_key"]))
        ).one()
        attempt_state=conn.execute(
            sa.select(attempts.c.attempt_state,attempts.c.authoritative)
            .where(attempts.c.attempt_id==str(row["active_attempt_id"]))
        ).one()
    if state.action_state!="OPEN" or state.active_attempt_id is not None or attempt_state.attempt_state!="ABANDONED_PRE_SUBMIT" or not attempt_state.authoritative:
        raise RuntimeError("EXPIRED_CLAIM_RELEASE_NOT_VERIFIED")
    result={"account_scope_validated":True,"date":str(day),
        "complete_broker_history":True,"broker_pages":r.get("_sep29_pages"),
        "broker_order_rows":0,"historical_db_accepted":0,"historical_db_fills":0,
        "code":"192820","previous_state":"IN_FLIGHT","new_state":"OPEN",
        "authoritative_proof":"COMPLETE_KIS_ZERO_ORDER_DAY_PLUS_DB_ATTEMPT",
        "broker_orders_created":0}
    print(json.dumps(result,ensure_ascii=False,indent=2))
    return result


if __name__=="__main__":
    run()
