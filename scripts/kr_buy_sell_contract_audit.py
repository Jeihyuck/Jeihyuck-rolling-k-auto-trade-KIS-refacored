"""Read-only, epoch-scoped KR BUY -> SELL lineage/contract audit.

Runs after KR CLOSE, never submits orders or reconciles historical executions.
An order ACK, hash echo, holdings snapshot and RECONCILED status are NOT broker
priced-fill evidence. Run with the credentialed account's DB environment.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.engine import get_engine
from trader.db.trading_epoch import active_trading_epoch_id

_NATIVE = {"KIS_ORDER_CUMULATIVE_EXECUTION", "KIS_EXECUTION", "KIS_DAILY_CCLD",
           "KIS_TRADE_EXECUTION", "KIS_ORDER_EXECUTION"}


def _obj(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError):
            pass
    return {}


def _sha(plan: dict) -> str:
    canonical = json.dumps(plan, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _records(conn: sa.Connection, statement: str, params: dict) -> list[dict]:
    return [dict(row) for row in conn.execute(sa.text(statement), params).mappings()]


def audit_buy(order: dict, *, positions: list[dict], fills: list[dict],
              sells: list[dict], sell_fills: list[dict], names: dict) -> dict:
    """Pure assessment: do not auto-join on a symbol, only exact owner/scope."""
    req = _obj(order.get("request_json"))
    plan = _obj(req.get("entry_exit_plan"))
    root_sha = str(req.get("entry_contract_sha256") or "")
    cycle = str(order.get("position_cycle_id") or "")
    exact = [
        p for p in positions if (
            p.get("code") == order.get("code")
            and p.get("strategy") == order.get("strategy")
            and str(p.get("position_cycle_id") or "") == cycle
            and str(p.get("portfolio_epoch_id") or "") == str(order.get("portfolio_epoch_id") or "")
            and str(p.get("trading_epoch_id") or "") == str(order.get("trading_epoch_id") or "")
        )
    ]
    same_code_other = [p for p in positions if p.get("code") == order.get("code")
                       and str(p.get("position_cycle_id") or "") != cycle]
    buy_fills = [f for f in fills if str(f.get("order_id")) == str(order.get("order_id"))]
    same_cycle_sells = [s for s in sells if (
        s.get("code") == order.get("code")
        and s.get("strategy") == order.get("strategy")
        and str(s.get("position_cycle_id") or "") == cycle
        and str(s.get("portfolio_epoch_id") or "") == str(order.get("portfolio_epoch_id") or "")
    )]
    sell_ids = {str(s["order_id"]) for s in same_cycle_sells if s.get("status") in ("FILLED", "RECONCILED")}
    sold = sum(int(s.get("qty") or 0) for s in same_cycle_sells if str(s.get("order_id")) in sell_ids)
    documented_sell_fills = [f for f in sell_fills if str(f.get("order_id")) in sell_ids]
    sell_filled = sum(int(f.get("qty") or 0) for f in documented_sell_fills)
    native_buy = sum(int(f.get("qty") or 0) for f in buy_fills
                     if _obj(f.get("fill_meta_json")).get("evidence_kind") in _NATIVE
                     and float(f.get("price") or 0) > 0)
    issues = []
    if len(exact) != 1:
        issues.append("CYCLE_DISCONNECTED" if same_code_other else "POSITION_MISSING")
    if len(exact) > 1:
        issues.append("MULTIPLE_POSITIONS_EXACT_CYCLE")
    pos = exact[0] if len(exact) == 1 else {}
    entry = _obj(pos.get("entry_meta_json"))
    stored_plan = _obj(pos.get("entry_exit_plan_json"))
    if pos:
        if not plan or stored_plan != plan:
            issues.append("ORIGINAL_PLAN_MISMATCH")
        if len(root_sha) != 64 or entry.get("entry_contract_sha256") != root_sha:
            issues.append("ORIGINAL_BUY_HASH_MISMATCH")
        if str(entry.get("source_buy_order_id") or "") != str(order.get("order_id") or ""):
            issues.append("SOURCE_BUY_ORDER_UNBOUND")
        expected = str(entry.get("entry_exit_plan_sha256") or "")
        if len(expected) != 64 or _sha(stored_plan) != expected:
            issues.append("STORED_PLAN_HASH_MISMATCH")
        if (pos.get("exit_policy_family") != plan.get("exit_policy_family")
                or pos.get("entry_style_selected") != plan.get("entry_style_selected")):
            issues.append("TOP_LEVEL_POLICY_DRIFT")
    if native_buy < int(order.get("qty") or 0):
        issues.append("ORIGINAL_PRICED_BUY_FILL_UNPROVEN")
    if sold != sell_filled:
        issues.append("SELL_ORDER_FILL_DISAGREEMENT")
    if pos and pos.get("status") == "CLOSED" and sold == 0:
        issues.append("CLOSED_WITHOUT_ORIGINAL_CYCLE_SELL")
    if pos and (sold + int(pos.get("qty") or 0) != int(order.get("qty") or 0)):
        issues.append("POSITION_QUANTITY_CHAIN_MISMATCH")
    if not issues:
        status = "EXPECTED"
    elif any(s in issues for s in ("ORIGINAL_PLAN_MISMATCH", "ORIGINAL_BUY_HASH_MISMATCH",
                                  "STORED_PLAN_HASH_MISMATCH", "TOP_LEVEL_POLICY_DRIFT")):
        status = "CONTRACT_MISMATCH"
    elif "CYCLE_DISCONNECTED" in issues or "CLOSED_WITHOUT_ORIGINAL_CYCLE_SELL" in issues:
        status = "CYCLE_DISCONNECTED"
    elif "ORIGINAL_PRICED_BUY_FILL_UNPROVEN" in issues:
        status = "FILL_EVIDENCE_MISSING"
    else:
        status = "REVIEW_REQUIRED"
    overlay = _obj(_obj(pos.get("position_meta")).get("kr_us_profit_contract"))
    return {
        "code": order.get("code"), "name": names.get(order.get("code")) or "NAME_UNAVAILABLE",
        "strategy_owner": "PB1", "buy_order_id": str(order.get("order_id")),
        "broker_order_no": order.get("kis_odno"), "buy_date": str(order.get("created_at") or "")[:10],
        "cycle": cycle, "portfolio_epoch_id": str(order.get("portfolio_epoch_id") or ""),
        "buy_qty": int(order.get("qty") or 0),
        "priced_buy_fill_qty_in_db": native_buy,
        "position_origin": pos.get("position_origin"), "position_status": pos.get("status"),
        "remaining_qty": pos.get("qty"),
        "sell_order_qty_same_cycle": sold, "sell_filled_qty_same_cycle": sell_filled,
        "original_contract_sha256": root_sha or None,
        "position_contract_sha256": entry.get("entry_contract_sha256"),
        "effective_amendment_version": overlay.get("version"),
        "effective_amendment_mode": overlay.get("activation_mode"),
        "status": status, "issues": sorted(set(issues)),
        "broker_original_day_independent_proof": "NOT_CHECKED",
    }


def collect(engine: sa.Engine, *, env: str) -> dict:
    account_key = get_account_key(env=env)
    epoch = str(active_trading_epoch_id(
        engine, env=env, account_id=account_key, required=True,
    ))
    if not epoch or epoch == "None":
        raise RuntimeError("KR_CONTRACT_AUDIT_ACTIVE_EPOCH_UNAVAILABLE")
    params = {"env": env, "epoch": epoch}
    with engine.connect() as conn:
        buys = _records(conn, """
            SELECT order_id, env, strategy, code, market, side, status, qty,
                created_at, kis_odno, position_cycle_id, portfolio_epoch_id,
                trading_epoch_id, request_json, response_json
            FROM public.orders
            WHERE env=:env AND trading_epoch_id=:epoch
                AND strategy='pb1_pullback_close' AND side='BUY'
                AND request_json::jsonb->>'enforce_entry_contract'='true'
            ORDER BY created_at, order_id
        """, params)
        positions = _records(conn, """
            SELECT code, strategy, trading_epoch_id, portfolio_epoch_id,
                position_cycle_id, status, position_origin, qty,
                entry_style_selected, exit_policy_family, entry_exit_plan_json,
                entry_meta_json, position_meta
            FROM public.positions WHERE env=:env AND trading_epoch_id=:epoch
        """, params)
        fills = _records(conn, """
            SELECT order_id, code, side, qty, price, fill_meta_json,
                kis_odno, position_cycle_id
            FROM public.fills
            WHERE env=:env AND trading_epoch_id=:epoch AND side='BUY'
        """, params)
        sell_fills = _records(conn, """
            SELECT order_id, code, side, qty, price, kis_odno, position_cycle_id
            FROM public.fills
            WHERE env=:env AND trading_epoch_id=:epoch AND side='SELL'
        """, params)
        sells = _records(conn, """
            SELECT order_id, code, strategy, status, qty, kis_odno,
                position_cycle_id, portfolio_epoch_id
            FROM public.orders
            WHERE env=:env AND trading_epoch_id=:epoch AND side='SELL'
        """, params)
        names = {str(x["code"]): x["name"] for x in _records(conn, """
            SELECT stock_code AS code, MIN(name) AS name
            FROM public.universe_members GROUP BY stock_code
        """, {})}
        infinite = _records(conn, """
            SELECT strategy_id, symbol, cycle_id, status, units_used,
                last_buy_date, last_buy_price, metadata
            FROM public.kr_infinite_state WHERE trading_epoch_id=:epoch
        """, params)
    reports = [
        audit_buy(b, positions=positions, fills=fills, sells=sells,
                  sell_fills=sell_fills, names=names)
        for b in buys
    ]
    statuses = Counter(row["status"] for row in reports)
    imported = [
        {"code": p["code"], "name": names.get(p["code"], "NAME_UNAVAILABLE"),
         "position_cycle_id": p["position_cycle_id"], "qty": p["qty"],
         "status": "IMPORTED_OR_UNPROVEN_NOT_AUTO_BOUND"}
        for p in positions if p.get("position_origin") == "IMPORTED" and p.get("status") == "OPEN"
    ]
    return {
        "version": "KR_BUY_SELL_READONLY_AUDIT_V1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "env": env, "trading_epoch_id": epoch,
        "account": "REDACTED",
        "signed_pb1_buy_count": len(reports),
        "original_contract_position_matched": sum(
            x["position_contract_sha256"] == x["original_contract_sha256"]
            and x["position_contract_sha256"] is not None
            and not any(i in x["issues"] for i in
                ("ORIGINAL_PLAN_MISMATCH", "STORED_PLAN_HASH_MISMATCH",
                 "SOURCE_BUY_ORDER_UNBOUND", "TOP_LEVEL_POLICY_DRIFT"))
            for x in reports),
        "categories": dict(statuses), "buys": reports,
        "imported_open_positions": imported,
        "infinite_owner": [
            {"symbol": x["symbol"], "strategy_id": x["strategy_id"],
             "cycle": x["cycle_id"], "status": x["status"],
             "units_used": x["units_used"],
             "adaptive_exit_policy": "OWNER_KR_INFINITE_V1_UNCHANGED"}
            for x in infinite if x.get("strategy_id") == "KR_INFINITE_V1"
        ],
        "KIS_live_holdings": "NOT_CHECKED",
        "historical_fill_repair": "NONE_READ_ONLY",
        "disclaimer": "Same-day DB evidence is not independent KIS original-order proof or a historical no-mutation timeline.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="practice", choices=["practice", "real"])
    parser.add_argument("--output", default="")
    args = parser.parse_args(argv)
    try:
        report = collect(get_engine(), env=args.env)
        print("[KR_CONTRACT_AUDIT][SUMMARY] signed=%s matched=%s categories=%s" % (
            report["signed_pb1_buy_count"], report["original_contract_position_matched"],
            json.dumps(report["categories"], ensure_ascii=False, sort_keys=True)))
        rc = 0
    except Exception as exc:
        report = {"version": "KR_BUY_SELL_READONLY_AUDIT_V1",
                  "generated_utc": datetime.now(timezone.utc).isoformat(),
                  "status": "AUDIT_FAILED", "error_type": type(exc).__name__,
                  "account": "REDACTED"}
        print("[KR_CONTRACT_AUDIT][FAIL] type=%s" % type(exc).__name__)
        rc = 2
    if args.output:
        destination = Path(args.output)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = destination.with_name(destination.name + ".tmp." + str(os.getpid()))
        temp.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        temp.replace(destination)
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
