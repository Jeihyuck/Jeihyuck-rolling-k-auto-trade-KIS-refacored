"""One-time practice PB1 claim restoration; no broker calls, no SELL changes.

Historical day: 2026-10-19. 251970 has an explicit local pre-KIS
deadline failure. 192820 EXPIRED has no prebroker proof and MUST stay fenced.
Default: DB read only. --apply requires explicit broker-order disable.
"""
from __future__ import annotations
from datetime import date
import argparse
import hashlib
import json
import os
import sqlalchemy as sa


def _audit(engine):
    from trader.db.schema import schema_for_engine
    from trader.db.repos import _proven_kr_pb1_before_kis_request
    from trader.account_state import get_account_key
    schema = schema_for_engine(engine)
    account_digest = hashlib.sha256(get_account_key(env="practice").encode()).hexdigest()
    claims, attempts, orders = schema.execution_claims, schema.execution_attempts, schema.orders
    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(claims.c.action_key,claims.c.action_state,
                claims.c.active_attempt_id, attempts.c.attempt_state,
                attempts.c.authoritative,orders)
            .select_from(claims.join(attempts,sa.and_(
                claims.c.action_key==attempts.c.action_key,
                claims.c.active_attempt_id==attempts.c.attempt_id,
            )).join(orders,sa.and_(
                orders.c.env==claims.c.env,
                orders.c.client_order_key==attempts.c.client_order_key,
                orders.c.trading_epoch_id==claims.c.trading_epoch_id,
            )))
            .where(
                claims.c.env=="practice",claims.c.account_id==account_digest,
                claims.c.market=="KR",claims.c.strategy_owner=="PB1",
                claims.c.trade_date==date(2026,10,19),
                claims.c.action.like("BUY_ENTRY:PB1-%"),
                orders.c.code.in_(("251970","192820")),
            )
        ).mappings().all()
    result=[]
    for row in rows:
        order=dict(row)
        proof=_proven_kr_pb1_before_kis_request(order)
        result.append({
            "code":order["code"],"claim_state":order["action_state"],
            "order_state":order["status"],"attempt_state":order["attempt_state"],
            "has_active_attempt":bool(order["active_attempt_id"]),
            "local_pre_http_proof":bool(proof),
            "action":("RELEASE_PROVEN" if order["code"]=="251970" and proof else "RETAIN_FENCE"),
        })
    return sorted(result,key=lambda v:v["code"])


def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument("--apply",action="store_true")
    args=parser.parse_args(argv)
    if os.getenv("KIS_ENV","").lower()!="practice" or os.getenv("STRATEGY_ENV","").lower()!="practice":
        parser.error("PRACTICE_SCOPE_REQUIRED")
    if args.apply and (
        os.getenv("DISABLE_REAL_TRADING")!="1" or
        os.getenv("LIVE_TRADING_ENABLED")!="0" or
        os.getenv("NO_TRADE")!="1"
    ):
        parser.error("BROKER_ORDER_DISABLE_FLAGS_REQUIRED")
    from trader.db.engine import get_engine
    from trader.db.repos import OrdersRepo
    engine=get_engine()
    before=_audit(engine)
    code_map={r["code"]:r for r in before}
    if args.apply:
        if set(code_map)!={"251970","192820"}:
            raise RuntimeError("CLAIM_ORIGINAL_SCOPE_MISMATCH")
        a,b=code_map["251970"],code_map["192820"]
        if not(a["claim_state"]=="IN_FLIGHT" and a["local_pre_http_proof"] and
            a["attempt_state"]=="CREATED" and b["claim_state"]=="IN_FLIGHT" and
            not b["local_pre_http_proof"] and b["attempt_state"]=="CREATED"):
            raise RuntimeError("CLAIM_RECOVERY_PROOF_NOT_MATCHED")
        changed=OrdersRepo(engine).recover_proven_kr_pb1_prebroker_claims(
            env="practice",trade_date=date(2026,10,19),
        )
        if changed!=1:
            raise RuntimeError(f"CLAIM_RELEASE_COUNT_MISMATCH:{changed}")
    after=_audit(engine)
    # The released row drops from active-claim join by design.
    if args.apply:
        if any(r["code"]=="251970" for r in after):
            raise RuntimeError("PROVEN_PREHTTP_CLAIM_NOT_RELEASED")
        if not any(r["code"]=="192820" and r["claim_state"]=="IN_FLIGHT" for r in after):
            raise RuntimeError("AMBIGUOUS_EXPIRED_FENCE_UNEXPECTEDLY_REMOVED")
    print(json.dumps({"mode":"APPLY_ONLY_PROVEN_PREHTTP" if args.apply else "READ_ONLY",
       "before":before,"after":after},ensure_ascii=False,indent=2))
    return 0

if __name__=="__main__":
    raise SystemExit(main())
