"""Broker-evidenced settlement for outstanding PB1 protective stop debt.

Called only by the authorized Stage4 reconciliation writer *after* durable
fills and ACK reconciliation. Never infers terminality from local OPEN,
cancel ACK, a missing DB row, or a missing KIS original-date observation.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone

from sqlalchemy import text

logger=logging.getLogger(__name__)


def settle_verified_stop_debts_once(*, provider, trade_date: str,
                                    limit: int=4) -> dict:
    from trader.account_state import get_account_key, resolve_env_name
    from trader.us.db.repos import _get_engine_or_none
    from trader.us.execution.reconcile import reconcile_positions
    from trader.us.protective_tp_recovery import broker_proves_terminal_after_cancel
    from trader.us.protective_stop_obligation import (
        load_pending_stop_obligations,satisfy_stop_obligation_from_broker_proof,
    )

    if resolve_env_name()!="practice":
        raise RuntimeError("STAGE4_STOP_SETTLEMENT_PRACTICE_ONLY")
    # Never request an expensive broker balance merely to prove that no
    # protective debts exist. This worker is invoked every Stage4 cycle.
    debts=load_pending_stop_obligations(env="practice")
    if not debts:
        return {"checked":0,"settled":0,"blocked":0,"total_pending":0}
    account=str(get_account_key(env="practice"))
    broker_balance=reconcile_positions(provider=provider,trade_date=trade_date)
    if not (broker_balance.get("status")=="OK"
            and broker_balance.get("balance_fetch_status")=="OK"
            and broker_balance.get("authoritative_positions") is True
            and broker_balance.get("preserve_previous_positions") is False):
        raise RuntimeError("STAGE4_STOP_SETTLEMENT_BALANCE_UNVERIFIED")
    held={
        str(row.get("symbol") or "").upper():int(row.get("qty") or 0)
        for row in (broker_balance.get("positions") or [])
    }
    engine=_get_engine_or_none()
    if engine is None:
        raise RuntimeError("STAGE4_STOP_SETTLEMENT_DB_UNAVAILABLE")
    checked=0
    settled=0
    blocked=0
    for debt in debts[:min(20,max(1,int(limit)))]:
        symbol=str(debt.get("symbol") or "").upper()
        life=str(debt.get("lifecycle_id") or "")
        epoch=str(debt.get("trading_epoch_id") or "")
        if (not symbol or symbol=="TQQQ" or not life or not epoch
                or str(debt.get("account_id") or "")!=account):
            blocked+=1
            continue
        if held.get(symbol,0)!=0:
            continue
        checked+=1
        # Claim account_id is SHA256 of canonical broker account key.
        account_claim=hashlib.sha256(account.encode("utf-8")).hexdigest()
        with engine.connect() as conn:
            active=conn.execute(text("""
                SELECT COUNT(*)
                FROM us_execution_claims c
                WHERE c.env='practice' AND c.market='US'
                  AND c.trading_epoch_id=:epoch AND c.account_id=:account
                  AND c.strategy_owner='US_STANDARD' AND c.lifecycle_id=:life
                  AND c.action_state IN (
                      'IN_FLIGHT','UNCERTAIN','PARTIALLY_SATISFIED'
                  )
            """),{"epoch":epoch,"account":account_claim,"life":life}).scalar_one()
            if int(active or 0)>0:
                blocked+=1
                continue
            orders=conn.execute(text("""
                SELECT DISTINCT o.trade_date,o.symbol,o.order_no,
                       o.qty_requested,o.qty_filled,o.env,o.trading_epoch_id
                FROM us_orders o
                JOIN us_execution_claims c
                  ON c.trading_epoch_id=o.trading_epoch_id
                 AND c.env=o.env AND c.trade_date=o.trade_date
                 AND c.lifecycle_id=:life AND c.account_id=:account
                 AND c.strategy_owner='US_STANDARD'
                 AND c.action=upper(o.meta->>'profit_capture_stage')
                WHERE o.env='practice' AND o.trading_epoch_id=:epoch
                  AND o.symbol=:symbol AND o.side='SELL'
                  AND o.order_no IS NOT NULL
                ORDER BY o.trade_date
                LIMIT 50
            """),{"life":life,"account":account_claim,"epoch":epoch,
                   "symbol":symbol}).mappings().all()
        # No KIS original-order coverage is NOT broker-open-orders=0 proof.
        if not orders or len(orders)>=50:
            blocked+=1
            continue
        originals=[]
        for order_row in orders:
            order=dict(order_row)
            evidence=provider.get_fills_by_order_no(
                order_no=str(order["order_no"]),symbol=symbol,
                trade_date=str(order["trade_date"]),
            )
            if not broker_proves_terminal_after_cancel(order,evidence):
                originals=[]
                break
            originals.append({"order_no":str(order["order_no"]),
                              "trade_date":str(order["trade_date"]),
                              "status":str(evidence["status"]),
                              "filled_qty":int(evidence["filled_qty"]),
                              "broker_open_qty":int(evidence["remaining_qty"])})
        if not originals:
            blocked+=1
            continue
        now=datetime.now(timezone.utc).isoformat()
        receipt=hashlib.sha256(
            json.dumps(originals,sort_keys=True).encode("utf-8")
        ).hexdigest()
        proof={
            "env":"practice","account_id":account,
            "trading_epoch_id":epoch,"strategy_owner":"US_STANDARD",
            "symbol":symbol,"lifecycle_id":life,
            "position_source":"KIS_AUTHORITATIVE",
            "open_orders_source":"KIS_AUTHORITATIVE",
            "fills_reconciled":True,"claims_reconciled":True,
            "broker_terminal_order_states":True,
            "broker_holding_qty":0,"broker_open_sell_qty":0,
            "broker_evidence_ref":"KIS-original-date:"+receipt,
            "observed_at":now,"terminal_orders":originals,
        }
        if satisfy_stop_obligation_from_broker_proof(debt=debt,proof=proof):
            settled+=1
        else:
            blocked+=1
    result={"checked":checked,"settled":settled,"blocked":blocked,
            "total_pending":len(debts)}
    logger.info("[US_STAGE4][STOP_SETTLEMENT] %s",result)
    return result
