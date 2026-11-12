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
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.engine import get_engine
from trader.db.trading_epoch import active_trading_epoch_id

_NATIVE = {"KIS_ORDER_CUMULATIVE_EXECUTION", "KIS_INDIVIDUAL_EXECUTION"}


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


def _kst_day(value: Any) -> str:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        return "UNKNOWN_DATE"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(ZoneInfo("Asia/Seoul")).date().isoformat()


def _priced(value: Any) -> bool:
    try:
        val = Decimal(str(value))
        return val.is_finite() and val > 0
    except (InvalidOperation, ValueError, TypeError):
        return False


def _sha(plan: dict) -> str:
    canonical = json.dumps(plan, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _records(conn: sa.Connection, statement: str, params: dict) -> list[dict]:
    return [dict(row) for row in conn.execute(sa.text(statement), params).mappings()]


def _same_scope(a: dict, b: dict, *, owner: bool = True) -> bool:
    if not all(str(a.get(k) or "").strip() for k in (
        "env", "market", "code", "trading_epoch_id",
        "portfolio_epoch_id", "position_cycle_id"
    )):
        return False
    keys = ("env", "market", "code", "trading_epoch_id",
            "portfolio_epoch_id", "position_cycle_id")
    if owner:
        keys += ("strategy",)
    return all(str(a.get(k) or "") == str(b.get(k) or "") for k in keys)


def _required_scope(order: dict) -> bool:
    keys = ("order_id", "env", "strategy", "market", "code",
            "trading_epoch_id", "portfolio_epoch_id", "position_cycle_id")
    return all(str(order.get(k) or "").strip() for k in keys)


def audit_buy(order: dict, *, positions: list[dict], fills: list[dict],
              sells: list[dict], sell_fills: list[dict], names: dict,
              cycle_buy_orders: list[dict] | None = None) -> dict:
    """Audit one signed PB1 BUY without assigning all cycle sells to that BUY.

    If multiple entry orders share a lifecycle, report cycle allocation debt
    rather than misattributing every sell to every entry. KIS cumulative fill
    evidence is not a native individual execution timestamp.
    """
    from trader.db.repos import _assert_kr_buy_entry_contract
    from trader.kr.order_execution_evidence import summarize_native_order, _odno

    req = _obj(order.get("request_json"))
    plan = _obj(req.get("entry_exit_plan"))
    root_sha = str(req.get("entry_contract_sha256") or "")
    issues: list[str] = []
    scope_ok = _required_scope(order)
    if not scope_ok or not _odno(order.get("kis_odno")):
        issues.append("ORIGINAL_BUY_IDENTITY_INCOMPLETE")
    try:
        _assert_kr_buy_entry_contract(req)
    except (RuntimeError, ValueError, TypeError) as exc:
        issues.append("ORIGINAL_BUY_CONTRACT_INVALID:" + str(exc)[:120])
    echo = _obj(_obj(order.get("response_json")).get("_kr_buy_entry_contract"))
    if echo.get("entry_contract_sha256") != root_sha or not root_sha:
        issues.append("ORDER_CONTRACT_ECHO_UNVERIFIED")

    exact = [p for p in positions if scope_ok and _same_scope(order, p)]
    if len(exact) != 1:
        siblings = [p for p in positions
                    if p.get("env") == order.get("env") and p.get("code") == order.get("code")
                    and p.get("trading_epoch_id") == order.get("trading_epoch_id")]
        issues.append("CYCLE_DISCONNECTED" if siblings else "POSITION_MISSING")
    if len(exact) > 1:
        issues.append("MULTIPLE_POSITIONS_EXACT_CYCLE")
    pos = exact[0] if len(exact) == 1 else {}
    entry = _obj(pos.get("entry_meta_json"))
    stored_plan = _obj(pos.get("entry_exit_plan_json"))
    if pos:
        if not plan or stored_plan != plan:
            issues.append("ORIGINAL_PLAN_MISMATCH")
        if entry.get("entry_contract_sha256") != root_sha or len(root_sha) != 64:
            issues.append("ORIGINAL_BUY_HASH_MISMATCH")
        if str(entry.get("source_buy_order_id") or "") != str(order.get("order_id") or ""):
            issues.append("SOURCE_BUY_ORDER_UNBOUND")
        expected = str(entry.get("entry_exit_plan_sha256") or "")
        if len(expected) != 64 or _sha(stored_plan) != expected:
            issues.append("STORED_PLAN_HASH_MISMATCH")
        if (pos.get("exit_policy_family") != plan.get("exit_policy_family") or
                pos.get("entry_style_selected") != plan.get("entry_style_selected")):
            issues.append("TOP_LEVEL_POLICY_DRIFT")
        last_eval = _obj(pos.get("last_exit_eval_json"))
        eval_plan = _obj(last_eval.get("entry_exit_plan"))
        if eval_plan and eval_plan != plan:
            issues.append("SELL_EVALUATION_PLAN_DRIFT")
        if last_eval.get("entry_exit_plan_status") not in (None, "OK"):
            issues.append("SELL_EVALUATION_CONTRACT_STATUS_NOT_OK")
        if not eval_plan:
            issues.append("SELL_EVALUATION_NOT_IN_DB_SNAPSHOT")

    related_sells = [s for s in sells if scope_ok and _same_scope(order, s)]
    extra_sells = [s for s in sells if s.get("code") == order.get("code")
                   and s.get("trading_epoch_id") == order.get("trading_epoch_id")
                   and s.get("position_cycle_id") == order.get("position_cycle_id")
                   and not _same_scope(order, s)]
    if extra_sells:
        issues.append("SELL_SCOPE_OWNER_OR_MARKET_MISMATCH")
    buy_evidence = summarize_native_order(order, fills)
    issues.extend("BUY:" + issue for issue in buy_evidence["issues"])
    if buy_evidence["evidence_status"] not in (
        "ECONOMIC_AND_TIMESTAMP_PROVEN", "ECONOMIC_PROOF_TIMESTAMP_PROXY"
    ):
        issues.append("ORIGINAL_PRICED_BUY_FILL_UNPROVEN")
    sell_reports = [summarize_native_order(s, sell_fills) for s in related_sells]
    for sr in sell_reports:
        issues.extend("SELL:" + issue for issue in sr["issues"])
    unknown_sell = any(sr["proven_filled_qty"] is None for sr in sell_reports)
    unknown_sell |= any(sr["evidence_status"] in ("UNPROVEN", "CONFLICT") for sr in sell_reports)
    sold = None if unknown_sell else sum(int(sr["proven_filled_qty"] or 0) for sr in sell_reports)
    # The status FILLED/RECONCILED is not economic execution evidence.
    sell_order_qty = sum(int(s.get("qty") or 0) for s in related_sells)

    cycle_buy_orders = cycle_buy_orders if cycle_buy_orders is not None else [order]
    cycle_buys = [b for b in cycle_buy_orders if scope_ok and _same_scope(order, b)]
    if len(cycle_buys) > 1:
        issues.append("MULTIPLE_BUYS_SAME_CYCLE_ALLOCATION_UNPROVEN")
    if pos and pos.get("status") == "CLOSED" and not related_sells:
        issues.append("CLOSED_WITHOUT_ORIGINAL_CYCLE_SELL")
    if pos and len(cycle_buys) == 1:
        buy_filled = buy_evidence["proven_filled_qty"]
        if buy_filled is not None and sold is not None:
            if buy_filled - sold != int(pos.get("qty") or 0):
                issues.append("POSITION_ECONOMIC_QUANTITY_CHAIN_MISMATCH")
        else:
            issues.append("POSITION_ECONOMIC_CHAIN_UNPROVEN")

    if any(i in issues for i in (
        "ORIGINAL_PLAN_MISMATCH", "ORIGINAL_BUY_HASH_MISMATCH",
        "STORED_PLAN_HASH_MISMATCH", "TOP_LEVEL_POLICY_DRIFT",
        "SELL_EVALUATION_PLAN_DRIFT"
    )):
        status = "CONTRACT_MISMATCH"
    elif "CYCLE_DISCONNECTED" in issues or "CLOSED_WITHOUT_ORIGINAL_CYCLE_SELL" in issues:
        status = "CYCLE_DISCONNECTED"
    elif "ORIGINAL_PRICED_BUY_FILL_UNPROVEN" in issues:
        status = "FILL_EVIDENCE_MISSING"
    elif issues or (buy_evidence["time_quality"] != "NATIVE_EXECUTION_TIMESTAMP"
                    or any(sr["time_quality"] != "NATIVE_EXECUTION_TIMESTAMP" for sr in sell_reports)):
        status = "REVIEW_REQUIRED"
    else:
        status = "EXPECTED"

    overlay = _obj(_obj(pos.get("position_meta")).get("kr_us_profit_contract"))
    return {
        "code": order.get("code"),
        "name": names.get(order.get("code")) or "NAME_UNAVAILABLE",
        "strategy_owner": "PB1", "buy_order_id": str(order.get("order_id") or ""),
        "broker_order_no": order.get("kis_odno"),
        "buy_date": _kst_day(order.get("created_at")),
        "cycle": str(order.get("position_cycle_id") or ""),
        "portfolio_epoch_id": str(order.get("portfolio_epoch_id") or ""),
        "submitted_buy_qty": int(order.get("qty") or 0),
        "proven_buy_fill_qty": buy_evidence["proven_filled_qty"],
        "buy_time_quality": buy_evidence["time_quality"],
        "buy_evidence_status": buy_evidence["evidence_status"],
        "position_origin": pos.get("position_origin"),
        "position_status": pos.get("status"),
        "remaining_qty": pos.get("qty"),
        "same_cycle_buy_orders": len(cycle_buys),
        "sell_submitted_qty_same_cycle": sell_order_qty,
        "sell_proven_filled_qty_same_cycle": sold,
        "sell_evidence": sell_reports,
        "original_contract_sha256": root_sha or None,
        "position_contract_sha256": entry.get("entry_contract_sha256"),
        "last_sell_evaluation_plan_matches_buy": (
            _obj(_obj(pos.get("last_exit_eval_json")).get("entry_exit_plan")) == plan
            if _obj(pos.get("last_exit_eval_json")).get("entry_exit_plan") else None
        ),
        "sell_evaluation_history_complete": False,
        "effective_amendment_version": overlay.get("version"),
        "effective_amendment_mode": overlay.get("activation_mode"),
        "status": status, "issues": sorted(set(issues)),
        "broker_original_day_independent_proof": "NOT_CHECKED",
    }


def collect(engine: sa.Engine, *, env: str) -> dict:
    """Audit all current-epoch scopes and separate signed/unsigned owners."""
    account_key = get_account_key(env=env)
    epoch = str(active_trading_epoch_id(
        engine, env=env, account_id=account_key, required=True,
    ))
    if not epoch or epoch == "None":
        raise RuntimeError("KR_CONTRACT_AUDIT_ACTIVE_EPOCH_UNAVAILABLE")
    params = {"env": env, "epoch": epoch, "account_id": account_key}
    with engine.connect() as conn:
        bound = _records(conn, """
            SELECT trading_epoch_id FROM public.trading_epochs
            WHERE env=:env AND account_id=:account_id AND status='ACTIVE'
                AND trading_epoch_id=:epoch
        """, params)
        if len(bound) != 1:
            raise RuntimeError("KR_CONTRACT_AUDIT_ACCOUNT_EPOCH_NOT_UNIQUE")
        portfolios = _records(conn, """
            SELECT portfolio_epoch_id, account_id, env, strategy
            FROM public.portfolio_epochs
            WHERE env=:env
        """, params)
        portfolio_map = {str(x["portfolio_epoch_id"]): x for x in portfolios}
        buys = _records(conn, """
            SELECT order_id, env, strategy, code, market, side, status, qty,
                created_at, kis_odno, position_cycle_id, portfolio_epoch_id,
                trading_epoch_id, request_json, response_json
            FROM public.orders
            WHERE env=:env AND trading_epoch_id=:epoch AND side='BUY'
            ORDER BY created_at, order_id
        """, params)
        sells = _records(conn, """
            SELECT order_id, env, strategy, market, code, status, side,
                qty, kis_odno, position_cycle_id, portfolio_epoch_id,
                trading_epoch_id, created_at, response_json
            FROM public.orders
            WHERE env=:env AND trading_epoch_id=:epoch AND side='SELL'
            ORDER BY created_at, order_id
        """, params)
        positions = _records(conn, """
            SELECT env, market, code, strategy, trading_epoch_id,
                portfolio_epoch_id, position_cycle_id, status, position_origin, qty,
                entry_style_selected, exit_policy_family, entry_exit_plan_json,
                entry_meta_json, position_meta, last_exit_eval_json
            FROM public.positions WHERE env=:env AND trading_epoch_id=:epoch
        """, params)
        all_fills = _records(conn, """
            SELECT order_id, env, code, market, side, qty, price,
                fill_meta_json, kis_odno, position_cycle_id, portfolio_epoch_id,
                trading_epoch_id, broker_fill_id, trade_id, filled_at, created_at
            FROM public.fills WHERE env=:env AND trading_epoch_id=:epoch
        """, params)
        names = {str(x["code"]): x["name"] for x in _records(conn, """
            SELECT stock_code AS code, MIN(name) AS name
            FROM public.universe_members GROUP BY stock_code
        """, {})}
        infinite = _records(conn, """
            SELECT strategy_id, symbol, cycle_id, status, units_used,
                last_buy_date, last_buy_price, policy_version, metadata
            FROM public.kr_infinite_state WHERE trading_epoch_id=:epoch
        """, params)
        infinite_intents = _records(conn, """
            SELECT strategy_id, symbol, cycle_id, trade_date, side, status,
                requested_qty, filled_qty, requested_notional_krw,
                filled_notional_krw, broker_order_id, metadata
            FROM public.kr_infinite_order_intents
            WHERE trading_epoch_id=:epoch
        """, params)

    signed = [
        b for b in buys if b.get("strategy") == "pb1_pullback_close"
        and _obj(b.get("request_json")).get("enforce_entry_contract") is True
    ]
    reports = [audit_buy(b, positions=positions,
                         fills=[f for f in all_fills if f.get("side") == "BUY"],
                         sells=sells,
                         sell_fills=[f for f in all_fills if f.get("side") == "SELL"],
                         names=names, cycle_buy_orders=buys)
               for b in signed]
    for b, report in zip(signed, reports):
        port = portfolio_map.get(str(b.get("portfolio_epoch_id") or ""))
        if (not port or port.get("account_id") != account_key
                or port.get("strategy") != b.get("strategy")):
            report["issues"].append("PORTFOLIO_OWNER_ACCOUNT_MISMATCH_OR_UNKNOWN")
            report["status"] = "REVIEW_REQUIRED"
        report["issues"] = sorted(set(report["issues"]))

    categories = Counter(x["status"] for x in reports)
    unsigned = [
        {"order_id": str(b.get("order_id") or ""), "code": b.get("code"),
         "name": names.get(b.get("code"), "NAME_UNAVAILABLE"),
         "strategy": b.get("strategy"), "status": "CONTRACT_NOT_SIGNED_OR_LEGACY"}
        for b in buys if b not in signed
    ]
    imported = [
        {"code": p["code"], "name": names.get(p["code"], "NAME_UNAVAILABLE"),
         "strategy": p.get("strategy"), "position_cycle_id": p["position_cycle_id"],
         "qty": p["qty"], "status": "IMPORTED_NOT_AUTOMATICALLY_BOUND"}
        for p in positions if p.get("position_origin") == "IMPORTED"
    ]
    return {
        "version": "KR_BUY_SELL_READONLY_AUDIT_V2",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "audit_run_id": os.environ.get("KR_CONTRACT_AUDIT_RUN_ID") or
                        "MANUAL_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f"),
        "trade_date_kst": datetime.now(ZoneInfo("Asia/Seoul")).date().isoformat(),
        "audit_status": "COMPLETED_WITH_EVIDENCE_GAPS" if (
            unsigned or imported or any(x["status"] != "EXPECTED" for x in reports)
        ) else "COMPLETED",
        "env": env, "trading_epoch_id": epoch, "account": "REDACTED",
        "population": {
            "all_buy_orders": len(buys), "all_sell_orders": len(sells),
            "signed_pb1_buy_orders": len(signed),
            "unsigned_or_other_owner_buy_orders": len(unsigned),
            "all_positions": len(positions), "imported_positions": len(imported),
            "kr_infinite_order_intents": len(infinite_intents),
            "all_fill_rows": len(all_fills),
        },
        "signed_pb1_buy_count": len(reports),
        "original_contract_position_matched": sum(
            x["position_contract_sha256"] == x["original_contract_sha256"]
            and x["position_contract_sha256"] is not None
            and not any(i in x["issues"] for i in (
                "ORIGINAL_PLAN_MISMATCH", "STORED_PLAN_HASH_MISMATCH",
                "SOURCE_BUY_ORDER_UNBOUND", "TOP_LEVEL_POLICY_DRIFT",
                "SELL_EVALUATION_PLAN_DRIFT", "CYCLE_DISCONNECTED",
                "POSITION_MISSING", "PORTFOLIO_OWNER_ACCOUNT_MISMATCH_OR_UNKNOWN"
            )) for x in reports),
        "categories": dict(categories), "buys": reports,
        "unsigned_or_other_owner_buys": unsigned, "imported_positions": imported,
        "infinite_owner": [
            {"symbol": x["symbol"], "strategy_id": x["strategy_id"],
             "cycle": x["cycle_id"], "status": x["status"],
             "units_used": x["units_used"], "original_policy_version": x["policy_version"],
             "adaptive_exit_policy": "OWNER_KR_INFINITE_V1_NOT_FULLY_AUDITED"}
            for x in infinite if x.get("strategy_id") == "KR_INFINITE_V1"
        ],
        "infinite_order_intents": [
            {"symbol": x["symbol"], "cycle": x["cycle_id"],
             "trade_date": str(x["trade_date"]), "side": x["side"],
             "status": x["status"], "requested_qty": x["requested_qty"],
             "recorded_filled_qty": x["filled_qty"],
             "broker_order_no": x["broker_order_id"],
             "broker_original_execution_proof": "NOT_CHECKED"}
            for x in infinite_intents if x.get("strategy_id") == "KR_INFINITE_V1"
        ],
        "KIS_live_holdings": "NOT_CHECKED",
        "historical_fill_repair": "NONE_READ_ONLY",
        "disclaimer": (
            "Order-level filled quantity is not submitted quantity. A cumulative KIS "
            "watermark proves economic quantity, not individual execution timestamps. "
            "Only the latest saved evaluator snapshot is available, never a full "
            "historical proof. No broker call or repair was performed."
        ),
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
        report = {"version": "KR_BUY_SELL_READONLY_AUDIT_V2",
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
