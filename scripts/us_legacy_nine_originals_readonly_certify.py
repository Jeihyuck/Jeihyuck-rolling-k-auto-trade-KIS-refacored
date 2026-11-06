#!/usr/bin/env python3
"""US practice legacy nine-position original broker certification (GET-only).

This does not authorize trading or rebind historical account/portfolio scopes.
It proves only what authenticated original-day KIS order books, a complete
current KIS balance and archived same-trading-epoch DB rows actually support.

WSL practice invocation (no order/cancel):
  KIS_ENV=practice WSL_RUN_MARKET=US NO_TRADE=1 DRY_RUN=1 \
  US_ORDER_ARMED=0 DISABLE_REAL_TRADING=1 \
  .venv/bin/python -m scripts.us_legacy_nine_originals_readonly_certify \
    --trade-date 2026-11-05 --confirm-practice-readonly
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text

SYMBOLS = ("ABBV", "DHR", "HCA", "KO", "KR", "MCK", "PFE", "PG", "TMO")
TMO_ORIGINAL_ORDER_DATE = "2026-10-30"
TMO_ORIGINAL_ORDER_NO = "0000036019"
SELECT_POSITIONS = """
SELECT DISTINCT ON (symbol) symbol, qty, as_of, account_id,
    trading_epoch_id, portfolio_epoch_id, position_cycle_id
FROM public.us_positions
WHERE trading_epoch_id=:epoch AND as_of<=:trade_date
AND symbol=ANY(:symbols)
ORDER BY symbol,as_of DESC,created_at DESC
"""
SELECT_FILLS = """
SELECT symbol,side,trade_date,order_no,qty,meta
FROM public.us_fills
WHERE trading_epoch_id=:epoch AND symbol=ANY(:symbols)
AND side IN ('BUY','SELL') AND trade_date<=:trade_date
ORDER BY symbol,trade_date,order_no
"""
SELECT_ORIGINAL_ORDERS = """
SELECT symbol,side,trade_date,order_no,qty_requested,status,meta
FROM public.us_orders
WHERE trading_epoch_id=:epoch AND symbol=ANY(:symbols)
AND side IN ('BUY','SELL') AND trade_date<=:trade_date AND env='practice'
ORDER BY symbol,trade_date,order_no
"""


def _count(value: Any) -> int:
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("BROKER_OR_DB_QUANTITY_INVALID") from exc
    if not number.is_finite() or number < 0 or number != number.to_integral_value():
        raise ValueError("BROKER_OR_DB_QUANTITY_INVALID")
    return int(number)


def _plain(row: Any) -> dict:
    return dict(row)


def load_readonly_archive(engine: Any, *, trading_epoch_id: str,
                          trade_date: str) -> tuple[list[dict],list[dict],list[dict]]:
    if not trading_epoch_id:
        raise RuntimeError("US_EPOCH_UNVERIFIED")
    date.fromisoformat(trade_date)
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        args = {"epoch": trading_epoch_id, "trade_date": trade_date,
                "symbols": list(SYMBOLS)}
        positions = [_plain(x) for x in conn.execute(
            text(SELECT_POSITIONS),args).mappings().all()]
        fills = [_plain(x) for x in conn.execute(
            text(SELECT_FILLS),args).mappings().all()]
        orders = [_plain(x) for x in conn.execute(
            text(SELECT_ORIGINAL_ORDERS),args).mappings().all()]
    return positions, fills, orders


def prepare_original_plan(
    positions: list[dict], fills: list[dict], orders: list[dict],
) -> dict:
    """Build a narrow and fail-closed original order GET plan from DB rows."""
    blockers = []
    if len(positions) != len(SYMBOLS) or {x.get("symbol") for x in positions} != set(SYMBOLS):
        blockers.append("NINE_DISTINCT_CURRENT_POSITIONS_UNVERIFIED")
    pmap = {x.get("symbol"): x for x in positions}
    grouped: dict[str,list[dict]] = defaultdict(list)
    for row in fills:
        grouped[str(row.get("symbol") or "")].append(row)
    matching_orders: dict[tuple[str,str,str,str],list[dict]] = defaultdict(list)
    for order in orders:
        key = (str(order.get("symbol") or ""),
               str(order.get("side") or "").upper(),
               str(order.get("trade_date") or ""),
               str(order.get("order_no") or "").lstrip("0"))
        matching_orders[key].append(order)
    entries = []
    original_sell_entries = []
    for symbol in SYMBOLS:
        position = pmap.get(symbol)
        if not position:
            entries.append({"symbol":symbol,"status":"POSITION_ABSENT"})
            continue
        rows = grouped.get(symbol, [])
        buys = [f for f in rows if str(f.get("side")).upper()=="BUY"]
        sells = [f for f in rows if str(f.get("side")).upper()=="SELL"]
        try:
            net = sum(_count(f.get("qty")) for f in buys) - sum(
                _count(f.get("qty")) for f in sells)
            quantity = _count(position.get("qty"))
        except ValueError:
            net = None
            quantity = None
        if net is None or net != quantity:
            blockers.append(f"{symbol}_EPOCH_DB_NET_FILL_MISMATCH")
        if len(buys) != 1:
            blockers.append(f"{symbol}_ORIGINAL_BUY_NOT_UNIQUE")
            entries.append({"symbol":symbol,"status":"ORIGINAL_BUY_NOT_UNIQUE",
                            "epoch_db_net_matches":net == quantity if net is not None else False})
            continue
        buy = buys[0]
        meta = buy.get("meta") if isinstance(buy.get("meta"),dict) else {}
        order_no = str(buy.get("order_no") or "").strip()
        original_day = str(buy.get("trade_date") or "")
        key = (symbol,"BUY",original_day,order_no.lstrip("0"))
        matched = matching_orders.get(key,[])
        if len(matched) != 1:
            blockers.append(f"{symbol}_ORIGINAL_DB_ORDER_NOT_UNIQUE")
        try:
            q = _count(buy.get("qty"))
            requested = _count(meta.get("requested_qty"))
            order_requested = _count(matched[0].get("qty_requested")) if len(matched)==1 else 0
        except ValueError:
            q=requested=order_requested=0
        life = str(meta.get("position_lifecycle_id") or "")
        contract = str(meta.get("entry_exit_contract_sha256") or "")
        owner = str(meta.get("strategy_owner") or "").upper()
        original_order_meta = (
            matched[0].get("meta") if len(matched)==1 and
            isinstance(matched[0].get("meta"),dict) else {}
        )
        order_contract = str(original_order_meta.get("entry_exit_contract_sha256") or "")
        if (not order_no or len(original_day)!=10 or q<=0 or
            requested != q or order_requested != q or owner != "US_STANDARD" or
            not life or not contract or contract != order_contract):
            blockers.append(f"{symbol}_FROZEN_BUY_CONTRACT_NOT_PROVEN")
        for sale in sells:
            sale_day = str(sale.get("trade_date") or "")
            sale_no = str(sale.get("order_no") or "").strip()
            sale_meta = sale.get("meta") if isinstance(sale.get("meta"),dict) else {}
            sale_key = (symbol,"SELL",sale_day,sale_no.lstrip("0"))
            sale_orders = matching_orders.get(sale_key,[])
            try:
                sold = _count(sale.get("qty"))
                requested_sell = _count(sale_meta.get("requested_qty"))
                db_requested_sell = (
                    _count(sale_orders[0].get("qty_requested"))
                    if len(sale_orders)==1 else 0
                )
            except ValueError:
                sold=requested_sell=db_requested_sell=0
            # The original sell must belong to the original BUY's lifecycle.
            # All sale proofs are new KIS GETs; an existing DB FILLED status
            # without authenticated source-day broker quantity is not proof.
            sell_identity_matches = bool(
                sale_meta.get("position_lifecycle_id") == life and life
                and str(sale_meta.get("strategy_owner") or "").upper() == owner
            )
            sale_ok = bool(
                sale_no and len(sale_day)==10 and len(sale_orders)==1
                and 0 < sold <= requested_sell == db_requested_sell
                and sell_identity_matches
            )
            if not sale_ok:
                blockers.append(f"{symbol}_LEGACY_SELL_DB_LINEAGE_UNVERIFIED")
            original_sell_entries.append({
                "symbol":symbol, "trade_date":sale_day,
                "order_no":sale_no, "requested_qty":requested_sell,
                "db_filled_qty":sold, "db_order_status": (
                    str(sale_orders[0].get("status") or "").upper()
                    if len(sale_orders)==1 else "UNVERIFIED"
                ),
                "original_buy_lifecycle_matches":sell_identity_matches,
                "db_link_verified":sale_ok,
            })
        entries.append({
            "symbol":symbol, "trade_date":original_day, "order_no":order_no,
            "requested_qty":requested, "filled_qty":q,
            "strategy_owner":owner, "lifecycle_present":bool(life),
            "frozen_contract_match":bool(contract and contract==order_contract),
            "epoch_db_net_matches":net == quantity if net is not None else False,
            "db_qty":quantity,
            # Populated scope columns are not authenticated ORIGINAL scope
            # proof: they might have been copied from a newer portfolio.
            # Historical ownership requires a separately reviewed provenance
            # transition, including source-day KIS proof and immutable
            # portfolio registry timing. A GET-only audit never authorizes it.
            "scope_columns_present":bool(
                position.get("portfolio_epoch_id") and
                position.get("position_cycle_id") and position.get("account_id")
            ),
            "original_portfolio_proven":False,
        })
    return {"entries":entries,"original_sell_entries":original_sell_entries,
            "blockers":sorted(set(blockers)),
            "all_legacy_scope_proven":all(
                item.get("original_portfolio_proven") for item in entries)}


def check_current_balance(balance: Any, entries: list[dict]) -> dict:
    if (not isinstance(balance,dict) or balance.get("rt_cd")!="0" or
        balance.get("balance_complete") is not True or
        balance.get("balance_authoritative") is not True or
        balance.get("balance_conflicts")):
        return {"status":"CURRENT_KIS_BALANCE_INCOMPLETE"}
    output = balance.get("output1")
    if not isinstance(output,list):
        return {"status":"CURRENT_KIS_BALANCE_SCHEMA_UNVERIFIED"}
    actual={}
    for row in output:
        if not isinstance(row,dict):
            return {"status":"CURRENT_KIS_BALANCE_SCHEMA_UNVERIFIED"}
        symbol=str(row.get("ovrs_pdno") or row.get("pdno") or "").upper()
        if symbol not in SYMBOLS:
            continue
        if symbol in actual:
            return {"status":"CURRENT_KIS_BALANCE_DUPLICATE_SYMBOL","symbol":symbol}
        quantity = next((row.get(k) for k in (
            "ovrs_cblc_qty","cblc_qty","hldg_qty")
            if row.get(k) is not None),None)
        try:
            actual[symbol]=_count(quantity)
        except ValueError:
            return {"status":"CURRENT_KIS_BALANCE_QUANTITY_INVALID","symbol":symbol}
    mismatched=[]
    for entry in entries:
        symbol=entry["symbol"]
        if entry.get("db_qty") is None or actual.get(symbol) != entry["db_qty"]:
            mismatched.append(symbol)
    return {"status":"PASS" if not mismatched else "CURRENT_KIS_BALANCE_MISMATCH",
            "matching_symbols":len(SYMBOLS)-len(mismatched),
            "mismatched_symbols":mismatched}


def certify_get_only(plan: dict, client: Any) -> dict:
    """Authenticated KIS GET-only; not an account/epoch rebinding operation."""
    from scripts.us_original_order_readonly_probe import classify_original_order
    blockers=list(plan["blockers"])
    cache={}
    output=[]
    for item in plan["entries"]:
        if (not item.get("trade_date") or not item.get("order_no")
                or not item.get("requested_qty")):
            blockers.append(item["symbol"]+"_ORIGINAL_BOOK_CANNOT_BE_QUERIED")
            output.append({"symbol":item["symbol"],"status":"NO_ORIGINAL_KEY"})
            continue
        day=item["trade_date"]
        if day not in cache:
            try:
                cache[day]=client.get_us_today_orders(trade_date=day)
            except Exception as exc:
                cache[day]=None
                blockers.append("KIS_ORIGINAL_DAY_GET_"+type(exc).__name__)
        try:
            observation=classify_original_order(
                cache[day],order_no=item["order_no"],
                symbol=item["symbol"],expected_qty=item["requested_qty"],
                trade_date=day,side="BUY",
            )
            good=(observation["classification"]=="PROVEN_FILLED"
                  and observation.get("cumulative_filled_qty")==item["filled_qty"])
        except Exception:
            good=False
        if not good:
            blockers.append(item["symbol"]+"_ORIGINAL_BUY_BROKER_PROOF_MISSING")
        output.append({"symbol":item["symbol"],
                       "status":"PROVEN_FILLED" if good else "NOT_PROVEN",
                       "original_day":day})
    verified_sells=[]
    for sale in plan.get("original_sell_entries",[]):
        symbol=str(sale.get("symbol") or "")
        day=str(sale.get("trade_date") or "")
        if not sale.get("db_link_verified") or not day or not sale.get("order_no"):
            blockers.append(f"{symbol}_LEGACY_SELL_DB_LINEAGE_UNVERIFIED")
            verified_sells.append({"symbol":symbol,"status":"NOT_PROVEN"})
            continue
        if day not in cache:
            try:
                cache[day]=client.get_us_today_orders(trade_date=day)
            except Exception as exc:
                cache[day]=None
                blockers.append("KIS_ORIGINAL_SELL_GET_"+type(exc).__name__)
        try:
            proof=classify_original_order(
                cache[day],order_no=sale["order_no"],
                symbol=symbol,expected_qty=sale["requested_qty"],
                trade_date=day,side="SELL",
            )
            broker_classification=proof.get("classification")
            db_status=sale.get("db_order_status")
            terminal_contract_matches=(
                broker_classification=="PROVEN_FILLED"
                and db_status in {"FILLED","RECONCILED"}
                or broker_classification=="PROVEN_CANCELLED"
                and db_status in {"CANCELLED","EXPIRED"}
            )
            verified=bool(
                terminal_contract_matches
                and proof.get("cumulative_filled_qty")==sale["db_filled_qty"]
            )
        except Exception:
            verified=False
        if not verified:
            blockers.append(f"{symbol}_LEGACY_SELL_BROKER_PROOF_MISSING")
        verified_sells.append({
            "symbol":symbol,"original_day":day,
            "status":"PROVEN_TERMINAL" if verified else "NOT_PROVEN",
        })
    if TMO_ORIGINAL_ORDER_DATE not in cache:
        try:
            cache[TMO_ORIGINAL_ORDER_DATE]=client.get_us_today_orders(
                trade_date=TMO_ORIGINAL_ORDER_DATE)
        except Exception as exc:
            cache[TMO_ORIGINAL_ORDER_DATE]=None
            blockers.append("TMO_SELL_ORIGINAL_DAY_GET_"+type(exc).__name__)
    tmo=classify_original_order(
        cache[TMO_ORIGINAL_ORDER_DATE],order_no=TMO_ORIGINAL_ORDER_NO,
        symbol="TMO",expected_qty=1,trade_date=TMO_ORIGINAL_ORDER_DATE,
        side="SELL",
    )
    # A proven OPEN is trustworthy information but NOT proof that the
    # execution claim can be released or a new SELL submitted.
    if tmo["classification"] not in {"PROVEN_FILLED","PROVEN_CANCELLED"}:
        blockers.append("TMO_UNRESOLVED_ORIGINAL_SELL_NOT_TERMINAL")
    try:
        current=client.get_us_balance(force_refresh=True)
        balance=check_current_balance(current,plan["entries"])
    except Exception as exc:
        balance={"status":"CURRENT_KIS_BALANCE_GET_"+type(exc).__name__}
    if balance["status"]!="PASS":
        blockers.append(balance["status"])
    if not plan["all_legacy_scope_proven"]:
        blockers.append("ORIGINAL_PORTFOLIO_CYCLE_UNPROVEN_POLICY_REVIEW")
    broker_originals_and_balance_verified=(
        len(output)==len(SYMBOLS)
        and all(item.get("status")=="PROVEN_FILLED" for item in output)
        and len(verified_sells)==len(plan.get("original_sell_entries", []))
        and all(item.get("status")=="PROVEN_TERMINAL" for item in verified_sells)
        and tmo["classification"] in {"PROVEN_FILLED","PROVEN_CANCELLED"}
        and balance["status"]=="PASS"
    )
    # Report when the KIS source-day/BALANCE evidence is complete but the
    # portfolio's historical scope never existed in the legacy DB schema.
    # This makes read-only verification useful without conferring a broker
    # POST, claim-release or retroactive epoch adoption privilege.
    only_legacy_policy_pending=(
        broker_originals_and_balance_verified
        and set(blockers)=={"ORIGINAL_PORTFOLIO_CYCLE_UNPROVEN_POLICY_REVIEW"}
    )
    status=(
        "BROKER_SOURCE_PROVEN_LEGACY_POLICY_REQUIRED"
        if only_legacy_policy_pending
        else "READONLY_EVIDENCE_PASS" if not blockers else "NOT_PROVEN"
    )
    return {
        "broker_originals_and_balance_verified":broker_originals_and_balance_verified,
        "operational_order_go":False,
        "status":status,
        "broker_readonly_only":True,"broker_post_count":0,
        "db_update_count":0,"claims_released":0,
        "original_buy_results":output,
        "original_sell_results":verified_sells,
        "original_trade_dates_queried":sorted(cache),
        "tmo_original_sell":tmo["classification"],"balance":balance,
        "legacy_scope_rebind_approved":False,
        "blockers":sorted(set(blockers)),
    }


def assert_practice_client_matches_current_scope(client: Any, scope: dict) -> None:
    """The authenticated KIS client must belong to the SAME scoped DB account.

    Prior legacy BUY rows have NULL account_id. Matching only their symbol,
    order number and quantity can otherwise prove an unrelated practice
    account. Check CANO/product locally, print only an error classification.
    """
    from trader.account_state import get_account_key

    if (scope.get("env") != "practice"
            or scope.get("market") != "US"
            or not scope.get("trading_epoch_id")):
        raise RuntimeError("US_GO_DB_SCOPE_UNVERIFIED")
    cano = str(getattr(client, "_cano", "") or "")
    product = str(getattr(client, "_acnt_prdt_cd", "") or "")
    if not cano.isdigit() or not product.isdigit():
        raise RuntimeError("US_GO_KIS_CLIENT_ACCOUNT_UNVERIFIED")
    key = get_account_key(env="practice", cano=cano, product_code=product)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    if not scope.get("account_id") or digest != scope["account_id"]:
        raise RuntimeError("US_GO_KIS_ACCOUNT_SCOPE_MISMATCH")


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date",required=True)
    parser.add_argument("--confirm-practice-readonly",action="store_true")
    args=parser.parse_args()
    date.fromisoformat(args.trade_date)
    if (not args.confirm_practice_readonly or os.getenv("KIS_ENV")!="practice"
        or os.getenv("WSL_RUN_MARKET","").upper()!="US"
        or os.getenv("NO_TRADE")!="1"
        or os.getenv("DRY_RUN")!="1"
        or os.getenv("DISABLE_REAL_TRADING")!="1"
        or os.getenv("US_ORDER_ARMED","0")!="0"):
        print(json.dumps({"status":"US_PRACTICE_READONLY_ENV_REQUIRED"}))
        return 42
    try:
        from trader.us.db.repos import _us_execution_claim_scope,_get_engine_or_none
        from trader.us.execution.kis_us_client import KisUSClient
        scope=_us_execution_claim_scope()
        if scope.get("market")!="US" or scope.get("env")!="practice":
            raise RuntimeError("US_SCOPE_INVALID")
        engine=_get_engine_or_none()
        if engine is None:
            raise RuntimeError("DB_UNAVAILABLE")
        archive=load_readonly_archive(
            engine,trading_epoch_id=scope["trading_epoch_id"],
            trade_date=args.trade_date)
        plan=prepare_original_plan(*archive)
        client=KisUSClient(env="practice")
        assert_practice_client_matches_current_scope(client,scope)
        result=certify_get_only(plan,client)
    except Exception as exc:
        result={"status":"US_ORIGINAL_CERTIFICATION_FAILED",
                "error_type":type(exc).__name__,"broker_post_count":0,
                "db_update_count":0,"claims_released":0}
    print(json.dumps(result,sort_keys=True,ensure_ascii=False,default=str))
    # Exit zero only for authenticated GET evidence, NOT for actual trading.
    # operational_order_go stays False until an independently authorized
    # forward-only carry policy and native practice POST/ACK/FILL/CLOSE prove it.
    return 0 if result.get("status") in {
        "READONLY_EVIDENCE_PASS",
        "BROKER_SOURCE_PROVEN_LEGACY_POLICY_REQUIRED",
    } else 44


if __name__=="__main__":
    raise SystemExit(main())
