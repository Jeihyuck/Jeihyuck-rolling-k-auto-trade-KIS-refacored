"""Read-only original-day KIS evidence for unresolved historical KR PB1 BUYs.

No order submission, cancellation, fill/claim/position mutation or automatic
cross-cycle merge. A closed imported position must not be resurrected by a
generic historical reconcile. Output contains only scoped order economics
and identity facts, not the broker account or raw credentials.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.kr.recovery_scope_20261104 import (
    order_matches_recovery_scope, resolve_kr_recovery_scope,
)
from trader.kr.frozen_contract_recovery_260 import _broker_buy_cumulative_verified
from trader.kr.runtime_integrity_20260929 import _daily_ccld_all_pages_for_date
from trader.kr.broker_truth_hardening import _valid_original_kr_balance_snapshot

KST = ZoneInfo("Asia/Seoul")


def exact_buy_evidence(payload, *, day, code, odno, qty):
    if (not isinstance(payload, dict) or payload.get("rt_cd") != "0"
            or payload.get("_sep29_complete") is not True
            or payload.get("_diag_stub") or payload.get("_ccld_status")
            or not isinstance(payload.get("output1"), list)
            or any(not isinstance(row, dict) for row in payload["output1"])):
        return {"state": "NATIVE_HISTORY_UNVERIFIED"}
    matches = [row for row in payload["output1"]
               if str(row.get("odno") or "") == odno
               and str(row.get("pdno") or "") == code
               and str(row.get("ord_dt") or "") == day.strftime("%Y%m%d")]
    if len(matches) != 1:
        return {"state": "ORIGINAL_ORDER_NOT_UNIQUE", "match_count": len(matches)}
    row = matches[0]
    if not _broker_buy_cumulative_verified(row, code=code, order_number=odno,
                                           quantity=qty, trade_date=day):
        return {"state": "ORIGINAL_FULL_EXECUTION_NOT_PROVEN"}
    notional = Decimal(str(row["tot_ccld_amt"]))
    return {"state": "ORIGINAL_FULL_EXECUTION_PROVEN", "code": code, "odno": odno,
            "trade_date": day.isoformat(), "qty": qty,
            "notional": str(notional), "avg_price": str(notional/Decimal(qty)),
            "native_row_sha256": hashlib.sha256(json.dumps(row, sort_keys=True,
                                                           default=str).encode()).hexdigest()}


def collect(*, engine, kis, now, code=None, trade_date=None):
    if getattr(kis, "env", None) != "practice":
        raise RuntimeError("PRACTICE_BROKER_REQUIRED")
    scope = resolve_kr_recovery_scope(engine=engine, env="practice", kis=kis)
    if scope is None:
        raise RuntimeError("ACTIVE_ACCOUNT_EPOCH_REQUIRED")
    schema = schema_for_engine(engine)
    claims, attempts, orders, fills, pos = (
        schema.execution_claims, schema.execution_attempts, schema.orders,
        schema.fills, schema.positions,
    )
    account_hash = hashlib.sha256(scope.account_key.encode()).hexdigest()
    today = now.astimezone(KST).date()
    targets = []
    with engine.connect() as conn:
        query = sa.select(claims).where(
            claims.c.env == "practice", claims.c.market == "KR",
            claims.c.strategy_owner == "PB1", claims.c.account_id == account_hash,
            claims.c.trading_epoch_id == scope.trading_epoch_id,
            claims.c.action.like("BUY_ENTRY:%"), claims.c.trade_date < today,
            claims.c.action_state == "UNCERTAIN", claims.c.active_attempt_id.is_not(None),
        )
        if code:
            query = query.where(claims.c.lifecycle_id.like("%:"+code))
        if trade_date is not None:
            query = query.where(claims.c.trade_date == trade_date)
        for claim in conn.execute(query.order_by(claims.c.trade_date, claims.c.action_key).limit(8)).mappings():
            attempt = conn.execute(sa.select(attempts).where(
                attempts.c.action_key == claim["action_key"],
                attempts.c.attempt_id == claim["active_attempt_id"],
            )).mappings().one_or_none()
            if not attempt or not attempt.get("client_order_key"):
                continue
            matches = conn.execute(sa.select(orders).where(
                orders.c.env == scope.env, orders.c.trading_epoch_id == scope.trading_epoch_id,
                orders.c.client_order_key == attempt["client_order_key"], orders.c.side == "BUY",
            )).mappings().all()
            if len(matches) != 1:
                continue
            order = dict(matches[0])
            symbol = str(order["code"])
            if (symbol == "122630" or (code and symbol != code)
                    or str(claim["lifecycle_id"]).split(":")[-1] != symbol
                    or order["qty"] != claim["target_qty"]
                    or order["qty"] != attempt["requested_qty"]
                    or not order_matches_recovery_scope(conn=conn, schema=schema, order=order,
                        strategy="pb1_pullback_close", scope=scope)):
                continue
            odno = str(order.get("kis_odno") or "")
            if len(odno) != 10 or not odno.isascii() or not odno.isdigit():
                continue
            db_qty = conn.execute(sa.select(sa.func.coalesce(sa.func.sum(fills.c.qty), 0)).where(
                fills.c.order_id == order["order_id"], fills.c.env == scope.env,
                fills.c.trading_epoch_id == scope.trading_epoch_id, fills.c.side == "BUY",
            )).scalar_one()
            positions = conn.execute(sa.select(pos.c.position_cycle_id, pos.c.qty,
                pos.c.status, pos.c.position_origin).where(
                pos.c.env == scope.env, pos.c.trading_epoch_id == scope.trading_epoch_id,
                pos.c.portfolio_epoch_id == order["portfolio_epoch_id"], pos.c.code == symbol,
            )).mappings().all()
            targets.append({"claim": dict(claim), "order": order, "db_fill_qty": int(db_qty),
                            "positions": [dict(p) for p in positions]})
    # Include all prior-day signed source BUYs with missing native economics
    # or a missing exact lifecycle, not merely the currently UNCERTAIN claim.
    # This includes imported-and-already-closed positions: do not reopen them.
    from trader.db.repos import _assert_kr_buy_entry_contract
    from trader.kr.order_execution_evidence import summarize_native_order
    seen = {str(t["order"]["order_id"]) for t in targets}
    with engine.connect() as conn:
        source_query = sa.select(orders).where(
            orders.c.env == scope.env,
            orders.c.trading_epoch_id == scope.trading_epoch_id,
            orders.c.strategy == "pb1_pullback_close",
            orders.c.side == "BUY",
            orders.c.code != "122630",
        ).order_by(orders.c.created_at, orders.c.order_id)
        if code:
            source_query = source_query.where(orders.c.code == code)
        for raw in conn.execute(source_query).mappings():
            order = dict(raw)
            identity = str(order["order_id"])
            if identity in seen:
                continue
            created = order.get("submitted_at") or order.get("created_at")
            if not isinstance(created, datetime) or created.tzinfo is None:
                continue
            day = created.astimezone(KST).date()
            if day >= today or (trade_date is not None and day != trade_date):
                continue
            request = order.get("request_json")
            response = order.get("response_json")
            try:
                if isinstance(request, str):
                    request = json.loads(request)
                if isinstance(response, str):
                    response = json.loads(response)
                if (not isinstance(request, dict)
                        or request.get("enforce_entry_contract") is not True
                        or not isinstance(response, dict)
                        or request.get("entry_contract_sha256") !=
                        (response.get("_kr_buy_entry_contract") or {}).get("entry_contract_sha256")):
                    continue
                _assert_kr_buy_entry_contract(request)
            except (ValueError, TypeError, RuntimeError, AttributeError):
                continue
            if not order_matches_recovery_scope(conn=conn, schema=schema, order=order,
                        strategy="pb1_pullback_close", scope=scope):
                continue
            symbol = str(order["code"])
            odno = str(order.get("kis_odno") or "")
            if len(odno) != 10 or not odno.isascii() or not odno.isdigit():
                continue
            native = [dict(row) for row in conn.execute(sa.select(fills).where(
                fills.c.order_id == order["order_id"], fills.c.env == scope.env,
                fills.c.trading_epoch_id == scope.trading_epoch_id, fills.c.side == "BUY",
            )).mappings()]
            positions = [dict(row) for row in conn.execute(sa.select(
                pos.c.position_cycle_id, pos.c.qty, pos.c.status, pos.c.position_origin
            ).where(
                pos.c.env == scope.env, pos.c.trading_epoch_id == scope.trading_epoch_id,
                pos.c.portfolio_epoch_id == order["portfolio_epoch_id"], pos.c.code == symbol,
            )).mappings()]
            exact = [p for p in positions if str(p["position_cycle_id"]) == str(order["position_cycle_id"])]
            evidence = summarize_native_order(order, native)
            if len(exact) == 1 and evidence.get("proven_filled_qty") == order["qty"]:
                continue
            targets.append({"claim": {"trade_date": day, "action_key": None},
                            "order": order, "db_fill_qty": sum(int(f.get("qty") or 0) for f in native),
                            "positions": positions})
            seen.add(identity)
    targets.sort(key=lambda t: (
        any(str(p.get("position_cycle_id")) == str(t["order"].get("position_cycle_id"))
            for p in t["positions"]),
        str(t["claim"]["trade_date"]), str(t["order"]["order_id"])))
    proofs = []
    daily = {}
    failures = {}
    for target in targets:
        claim, order = target["claim"], target["order"]
        day = claim["trade_date"]
        if isinstance(day, str):
            day = date.fromisoformat(day)
        symbol = str(order["code"])
        odno = str(order["kis_odno"])
        try:
            if day in failures:
                raise RuntimeError("ORIGINAL_DAY_QUERY_ALREADY_FAILED")
            if day not in daily:
                if len(daily) + len(failures) >= 3:
                    proofs.append({"code": symbol, "odno": odno, "source_order_id": str(order["order_id"]), "state": "DEFERRED_DAY_BUDGET"})
                    continue
                try:
                    daily[day] = _daily_ccld_all_pages_for_date(kis, day)
                except Exception as exc:
                    failures[day] = type(exc).__name__
                    raise
            proof = exact_buy_evidence(daily[day], day=day, code=symbol, odno=odno,
                                       qty=int(order["qty"]))
        except Exception as exc:
            proof = {"state": "NATIVE_QUERY_FAILED", "error_type": type(exc).__name__}
        proofs.append({**proof, "code": symbol, "odno": odno,
                       "claim_action_key": claim["action_key"], "source_order_id": str(order["order_id"]),
                       "source_cycle": str(order["position_cycle_id"]),
                       "db_fill_qty": target["db_fill_qty"], "positions": target["positions"]})
    balance_started = datetime.now(timezone.utc)
    balance_ok = False
    quantities = {}
    try:
        snapshot = kis.get_balance_cached(force=True)
        at = getattr(kis, "_balance_cache_at", None)
        if (isinstance(at, datetime) and at.tzinfo is not None and at >= balance_started
                and _valid_original_kr_balance_snapshot(snapshot, at=at)
                and not snapshot.get("_diag_stub") and not snapshot.get("_stub")):
            quantities = {str(row["pdno"]).zfill(6): int(row["hldg_qty"]) for row in snapshot["output1"]}
            balance_ok = True
    except Exception:
        pass
    for proof in proofs:
        proof["fresh_balance_proven"] = balance_ok
        proof["current_holding_qty"] = quantities.get(proof["code"], 0) if balance_ok else None
        proof["recovery_authorized"] = False
        proof["reason"] = "EXACT_CYCLE_AND_CURRENT_UNFILLED_REVIEW_REQUIRED_NO_WRITE"
    return {"version": "KR_HISTORICAL_BUY_NATIVE_EVIDENCE_V2", "env": scope.env,
            "trading_epoch_id": scope.trading_epoch_id, "cases": proofs,
            "days_queried": len(daily) + len(failures), "orders_submitted": 0, "db_rows_modified": 0}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--code")
    parser.add_argument("--trade-date", type=date.fromisoformat)
    args = parser.parse_args(argv)
    if args.code and (len(args.code) != 6 or not args.code.isascii() or not args.code.isdigit()):
        parser.error("six-digit KR code required")
    if not all(os.getenv(key) == "practice" for key in ("KIS_ENV", "STRATEGY_ENV")):
        raise RuntimeError("PRACTICE_ONLY")
    if os.getenv("NO_TRADE") != "1" or os.getenv("DISABLE_REAL_TRADING") != "1":
        raise RuntimeError("EXPLICIT_NO_TRADE_REQUIRED")
    from settings import API_BASE_URL, KIS_ENV
    if KIS_ENV != "practice" or not str(API_BASE_URL).startswith("https://openapivts.koreainvestment.com"):
        raise RuntimeError("PRACTICE_ENDPOINT_REQUIRED")
    from trader.db.engine import get_engine
    from trader.kis_wrapper import KisAPI
    engine = get_engine()
    try:
        evidence = collect(engine=engine, kis=KisAPI(kis_env="practice"),
                           now=datetime.now(KST), code=args.code, trade_date=args.trade_date)
        print(json.dumps(evidence, ensure_ascii=False, default=str, indent=2))
        return 0 if evidence["cases"] and all(
            row["state"] == "ORIGINAL_FULL_EXECUTION_PROVEN" and row["fresh_balance_proven"]
            for row in evidence["cases"]) else 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
