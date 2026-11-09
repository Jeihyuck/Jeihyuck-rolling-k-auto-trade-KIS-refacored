"""Read-only KR BUY HASH -> priced KIS fill -> position cycle evidence audit.

Usage:
 PYTHONPATH=. python scripts/kr_20261109_economic_chain_audit.py \\
   --epoch <ACTIVE_TRADING_EPOCH_UUID> --codes 293490,059090,122630

Does not call broker order APIs or mutate any DB table.  It cannot prove
current MTS parity or reconstruct original KIS fills that are not persisted.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from typing import Any

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.engine import get_engine
from trader.db.trading_epoch import active_trading_epoch_id
from trader.kr.economic_chain_integrity import audit_kr_buy_identity


def _select(conn, sql: str, env: str, epoch: str, codes: list[str]) -> list[dict[str, Any]]:
    stmt = sa.text(sql).bindparams(sa.bindparam("codes", expanding=True))
    return [dict(row) for row in conn.execute(
        stmt, {"env": env, "epoch": epoch, "codes": codes}
    ).mappings()]


def collect_readonly_identity_audit(
    engine: sa.Engine, *, env: str, epoch: str, codes: list[str],
) -> dict[str, Any]:
    if env != "practice":
        raise RuntimeError("KR_ECONOMIC_CHAIN_AUDIT_PRACTICE_ONLY")
    if not epoch or not codes or any(not re.fullmatch(r"\d{6}", code) for code in codes):
        raise ValueError("Exact trading epoch and six-digit codes are required")
    current_epoch = active_trading_epoch_id(
        engine, env=env, account_id=get_account_key(env=env), required=True,
    )
    if str(current_epoch or "") != epoch:
        raise RuntimeError("KR_ECONOMIC_CHAIN_ACTIVE_ACCOUNT_EPOCH_MISMATCH")
    with engine.connect() as conn:
        orders = _select(conn, """
            SELECT order_id, code, side, qty, status, kis_odno, strategy,
                   client_order_key, position_cycle_id, portfolio_epoch_id,
                   trading_epoch_id, request_json, response_json, created_at
            FROM public.orders WHERE env=:env AND trading_epoch_id=:epoch
              AND code IN :codes AND side='BUY'
            ORDER BY created_at
        """, env, epoch, codes)
        fills = _select(conn, """
            SELECT order_id, code, side, qty, price, kis_odno,
                   trading_epoch_id, position_cycle_id, fill_meta_json
            FROM public.fills WHERE env=:env AND trading_epoch_id=:epoch
              AND code IN :codes AND side='BUY'
        """, env, epoch, codes)
        positions = _select(conn, """
            SELECT code, qty, status, position_origin, position_cycle_id,
                   trading_epoch_id, exit_policy_family
            FROM public.positions WHERE env=:env AND trading_epoch_id=:epoch
              AND code IN :codes
        """, env, epoch, codes)
        links = [dict(row) for row in conn.execute(sa.text("""
            SELECT ea.client_order_key, ec.action_key, ec.action_state,
                   ec.strategy_owner
            FROM public.execution_attempts ea
            JOIN public.execution_claims ec ON ec.action_key=ea.action_key
            WHERE ec.env=:env AND ec.trading_epoch_id=:epoch
              AND ec.market='KR'
        """), {"env": env, "epoch": epoch}).mappings()]
        infinite = _select(conn, """
            SELECT symbol, strategy_id, cycle_id, side, status,
                   requested_qty, filled_qty, broker_order_id
            FROM public.kr_infinite_order_intents
            WHERE trading_epoch_id=:epoch AND symbol IN :codes
        """, env, epoch, codes)

    fills_by_order = defaultdict(list)
    for fill in fills:
        fills_by_order[str(fill.get("order_id") or "")].append(fill)
    pos_by_code = defaultdict(list)
    for position in positions:
        pos_by_code[str(position.get("code") or "")].append(position)
    claims_by_client = defaultdict(dict)
    for link in links:
        key = str(link.get("client_order_key") or "")
        claims_by_client[key][str(link.get("action_key") or "")] = link

    audits = []
    for order in orders:
        key = str(order.get("client_order_key") or "")
        claims = list(claims_by_client.get(key, {}).values())
        claim = claims[0] if len(claims) == 1 else None
        report = audit_kr_buy_identity(
            order=order,
            fills=fills_by_order.get(str(order["order_id"]), []),
            positions=pos_by_code.get(str(order["code"]), []),
            claim=claim,
        )
        report["strategy"] = str(order.get("strategy") or "")
        if len(claims) > 1:
            report["issues"].append("CLAIM_ACTION_KEY_AMBIGUOUS")
            report["status"] = "CHAIN_INCOMPLETE"
        if any(
            intent["symbol"] == order["code"]
            and intent["broker_order_id"] == order.get("kis_odno")
            and intent["strategy_id"] == "KR_INFINITE_V1"
            for intent in infinite
        ):
            report["issues"].append("PB1_PASSIVE_MIRROR_OF_KR_INFINITE")
            report["status"] = "CHAIN_INCOMPLETE"
        report["issues"] = sorted(set(report["issues"]))
        audits.append(report)

    return {
        "scope": {"env": env, "trading_epoch_id": epoch, "codes": codes},
        "buy_orders_audited": len(audits),
        "incomplete": sum(r["status"] == "CHAIN_INCOMPLETE" for r in audits),
        "results": audits,
        "broker_live_parity": "NOT_CHECKED",
        "note": "DB-only read-only audit. Missing historical fills require exact original KIS proof.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KR 2026-11-09 economic chain read-only audit")
    parser.add_argument("--epoch", required=True)
    parser.add_argument("--codes", default="293490,059090,122630")
    parser.add_argument("--env", default="practice", choices=["practice"])
    args = parser.parse_args(argv)
    codes = sorted(set(code.strip() for code in args.codes.split(",") if code.strip()))
    report = collect_readonly_identity_audit(
        get_engine(), env=args.env, epoch=args.epoch, codes=codes,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 2 if report["incomplete"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
