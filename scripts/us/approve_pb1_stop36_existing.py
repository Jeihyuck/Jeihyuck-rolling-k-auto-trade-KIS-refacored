"""Audit and explicitly approve the 3%/6% PB1 revision for existing practice positions.

DRY RUN IS THE DEFAULT. This tool changes protective stop policy for already
held positions, so --apply requires human approval_ref and expected position
count. It never places KIS orders or mutates frozen entry/exit contracts.
"""
from __future__ import annotations

import argparse
import json
from sqlalchemy import text
from trader.us.db.repos import _get_engine_or_none, _active_us_epoch
from trader.us.entry_exit_contract import extract_us_entry_exit_contract, verify_us_entry_exit_contract


def eligible_rows(conn, epoch: str) -> list[dict]:
    rows = conn.execute(text("""
        WITH latest_pos AS (
            SELECT DISTINCT ON (symbol) symbol, as_of, qty, meta
            FROM us_positions
            WHERE trading_epoch_id=:epoch
            ORDER BY symbol, as_of DESC
        ), latest_lifecycle AS (
            SELECT DISTINCT ON (symbol) symbol,trade_date,state->'lifecycle' AS lifecycle
            FROM us_position_risk_state
            WHERE trading_epoch_id=:epoch AND state->'lifecycle' IS NOT NULL
            ORDER BY symbol,trade_date DESC
        )
        SELECT p.symbol,p.qty,p.meta,l.lifecycle
        FROM latest_pos p
        JOIN latest_lifecycle l ON l.symbol=p.symbol
        WHERE p.qty>0 AND p.symbol<>'TQQQ'
        ORDER BY p.symbol
    """),{"epoch":epoch}).mappings().all()
    eligible=[]
    for row in rows:
        symbol=str(row["symbol"] or "").upper()
        life=dict(row["lifecycle"] or {})
        if life.get("is_open") is not True or str(life.get("strategy_owner") or "").upper()!="US_STANDARD":
            continue
        account=str(life.get("account_id") or "").strip()
        lifecycle=str(life.get("lifecycle_id") or "").strip()
        if not account or not lifecycle:
            raise RuntimeError(f"STOP_REVISION_IDENTITY_MISSING:{symbol}")
        contract=extract_us_entry_exit_contract({"symbol":symbol,"strategy_owner":"US_STANDARD","meta":row["meta"]})
        if not contract or not verify_us_entry_exit_contract(contract):
            raise RuntimeError(f"STOP_REVISION_FROZEN_CONTRACT_UNVERIFIED:{symbol}")
        swing=(contract.get("management") or {}).get("swing") or {}
        if not (float(swing.get("soft_stop") or 0)==0.05 and float(swing.get("hard_stop") or 0)==0.08):
            # New entries already have 3/6; revisions target only 5/8 originals.
            continue
        eligible.append({"symbol":symbol,"account":account,"lifecycle":lifecycle,
                         "source_sha":contract["sha256"],"qty":int(row["qty"])})
    return eligible


def run(*, apply: bool, approval_ref: str, expected_count: int) -> dict:
    engine=_get_engine_or_none()
    if engine is None:
        raise RuntimeError("STOP_REVISION_DURABLE_DB_REQUIRED")
    if apply and (not approval_ref.strip() or expected_count<1):
        raise RuntimeError("STOP_REVISION_APPLY_REQUIRES_APPROVAL_AND_EXPECTED_COUNT")
    with engine.begin() as conn:
        epoch=_active_us_epoch(conn)
        if not epoch:
            raise RuntimeError("STOP_REVISION_ACTIVE_EPOCH_REQUIRED")
        eligible=eligible_rows(conn,epoch)
        if apply and len(eligible)!=expected_count:
            raise RuntimeError(f"STOP_REVISION_COUNT_CHANGED expected={expected_count} actual={len(eligible)}")
        if apply:
            for p in eligible:
                conn.execute(text("""
                    INSERT INTO us_stop_policy_revisions
                    (trading_epoch_id,env,account_id,strategy_owner,symbol,lifecycle_id,
                     revision_code,source_contract_sha256,soft_stop,hard_stop,
                     approval_ref,active,effective_at)
                    VALUES (:epoch,'practice',:account,'US_STANDARD',:symbol,:life,
                            'PB1_STOP_3_6_V1',:sha,0.03,0.06,:approval,TRUE,NOW())
                    ON CONFLICT DO NOTHING
                """),{"epoch":epoch,"account":p["account"],"symbol":p["symbol"],
                       "life":p["lifecycle"],"sha":p["source_sha"],"approval":approval_ref})
            count=conn.execute(text("""
                SELECT count(*) FROM us_stop_policy_revisions
                WHERE trading_epoch_id=:epoch AND env='practice' AND active IS TRUE
                  AND revision_code='PB1_STOP_3_6_V1'
            """),{"epoch":epoch}).scalar()
            if int(count or 0)<expected_count:
                raise RuntimeError("STOP_REVISION_APPLIED_COUNT_MISMATCH")
    return {"mode":"APPLIED" if apply else "DRY_RUN","eligible_count":len(eligible),
            "symbols":[p["symbol"] for p in eligible]}


def main() -> int:
    a=argparse.ArgumentParser(description="Existing PB1 5/8 -> 3/6 approved revision; no KIS orders")
    a.add_argument("--apply",action="store_true")
    a.add_argument("--approval-ref",default="")
    a.add_argument("--expected-count",type=int,default=0)
    args=a.parse_args()
    print(json.dumps(run(apply=args.apply,approval_ref=args.approval_ref,
                         expected_count=args.expected_count),sort_keys=True))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
