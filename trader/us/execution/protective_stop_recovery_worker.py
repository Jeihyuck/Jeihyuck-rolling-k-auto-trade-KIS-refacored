"""Independent, lifecycle-scoped TP-fence recovery consumer for durable stop debt.

Practice-only. This worker NEVER submits a BUY or a replacement SELL. It may
request KIS cancellation only when the existing broker-proven TP guard,
transactional cancel reservation and operator execution authorization all pass.
A terminal broker proof reconciles via the existing canonical DB observer.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from trader.us.protective_stop_obligation import load_pending_stop_obligations
from trader.us.protective_tp_recovery import request_protective_tp_cancel

logger=logging.getLogger(__name__)


def recovery_candidates(pending: list[dict]) -> list[dict]:
    """Turn only proven identity-scoped protective debt into recovery requests."""
    results=[]
    seen=set()
    for item in pending:
        symbol=str(item.get("symbol") or "").upper()
        life=str(item.get("lifecycle_id") or "")
        account=str(item.get("account_id") or "")
        severity=int(item.get("severity") or 0)
        if not life or not account or not symbol or symbol=="TQQQ" or severity<2:
            continue
        key=(account,symbol,life)
        if key in seen:
            continue
        seen.add(key)
        results.append({
            "symbol":symbol, "side":"SELL","exit_type":(
                "hard_stop_loss" if severity>=3 else "persistent_soft_stop_full_exit"
            ),
            "strategy":"us_pb1_exit","strategy_owner":"US_STANDARD",
            "position_lifecycle_id":life,
            "meta":{"strategy_owner":"US_STANDARD","position_lifecycle_id":life},
        })
    return results


def recover_once(*, provider=None,kis_client=None,trade_date: str,
                 execute_practice_cancel: bool=False, pending=None) -> dict:
    if os.getenv("KIS_ENV", "practice").lower()!="practice":
        raise RuntimeError("STOP_RECOVERY_WORKER_PRACTICE_ONLY")
    debts=list(pending if pending is not None else load_pending_stop_obligations(env="practice"))
    intents=recovery_candidates(debts)
    if not execute_practice_cancel:
        return {"status":"DRY_RUN","debt_count":len(debts),"recovery_candidates":len(intents),
                "broker_queries":0,"broker_cancels":0}
    if os.getenv("US_STOP_RECOVERY_AUTHORIZED","0")!="1":
        raise RuntimeError("STOP_RECOVERY_CANCEL_NOT_AUTHORIZED")
    if os.getenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED","0")!="1":
        raise RuntimeError("STOP_RECOVERY_TP_CANCEL_GATE_DISABLED")
    if not intents:
        return {"status":"NO_DUE_STOP_DEBT","debt_count":len(debts),"results":[]}
    if provider is None or kis_client is None:
        raise RuntimeError("STOP_RECOVERY_BROKER_CLIENT_REQUIRED")
    observed=request_protective_tp_cancel(
        intents=intents,provider=provider,kis_client=kis_client,
        trade_date=trade_date,env="practice",
    )
    return {"status":"RECONCILE_REQUIRED","debt_count":len(debts),
            "results":[{"symbol":x.get("symbol"),"status":x.get("status"),
                        "reason":x.get("reason","")} for x in observed]}


def main() -> int:
    parser=argparse.ArgumentParser(description="Practice-only, independent stop/TP recovery")
    parser.add_argument("--trade-date",default="")
    parser.add_argument("--execute-practice-cancel",action="store_true")
    args=parser.parse_args()
    trade_date=args.trade_date or datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    if args.execute_practice_cancel:
        from trader.us.data_provider import USDataProvider
        from trader.us.execution.kis_us_client import KisUSClient
        provider=USDataProvider(offline=False,env="practice")
        kis_client=KisUSClient(env="practice",offline=False)
    else:
        provider=kis_client=None
    report=recover_once(provider=provider,kis_client=kis_client,
                        trade_date=trade_date,execute_practice_cancel=args.execute_practice_cancel)
    print(json.dumps(report,sort_keys=True))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
