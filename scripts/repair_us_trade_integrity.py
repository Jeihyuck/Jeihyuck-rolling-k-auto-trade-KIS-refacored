#!/usr/bin/env python3
"""Audit and conservatively quarantine corrupt US practice trade records.

No symbol is guessed.  ``--apply`` is practice-only and only quarantines rows
whose identity contradicts an actual KIS fill keyed by broker order number.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def _blank(value) -> bool:
    return value is None or str(value).strip().lower() in {"", "none", "null"}


def _meta(row: dict) -> dict:
    value = row.get("meta") or {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _is_synthetic(row: dict) -> bool:
    meta = _meta(row)
    return bool(meta.get("is_synthetic") or meta.get("synthetic") or meta.get("synthetic_fill")
                or meta.get("fill_evidence_type") in {"BALANCE_DELTA_SYNTHETIC", "LEGACY_SYNTHETIC"})


def audit(orders: list[dict], fills: list[dict]) -> dict:
    key_symbols, order_symbols = defaultdict(set), defaultdict(set)
    actual_by_order = {}
    for row in orders + fills:
        if not _blank(row.get("client_order_key")):
            key_symbols[str(row["client_order_key"])].add(str(row.get("symbol") or "").upper())
        if not _blank(row.get("order_no")):
            order_symbols[str(row["order_no"])].add(str(row.get("symbol") or "").upper())
    for fill in fills:
        if not _is_synthetic(fill) and not _blank(fill.get("order_no")):
            actual_by_order[str(fill["order_no"])] = fill
    issues = []
    for table, rows in (("us_orders", orders), ("us_fills", fills)):
        for row in rows:
            reasons = []
            if _blank(row.get("client_order_key")): reasons.append("BLANK_CLIENT_ORDER_KEY")
            key = str(row.get("client_order_key") or "")
            ono = str(row.get("order_no") or "")
            if key and len(key_symbols[key]) > 1: reasons.append("CLIENT_KEY_MULTIPLE_SYMBOLS")
            if ono and len(order_symbols[ono]) > 1: reasons.append("ORDER_NO_MULTIPLE_SYMBOLS")
            actual = actual_by_order.get(ono)
            if actual and str(actual.get("symbol") or "").upper() != str(row.get("symbol") or "").upper(): reasons.append("KIS_SYMBOL_MISMATCH")
            if actual and str(actual.get("side") or "").upper() != str(row.get("side") or "").upper(): reasons.append("KIS_SIDE_MISMATCH")
            if table == "us_fills" and actual is not row and actual and _is_synthetic(row):
                reasons.append("SYNTHETIC_DUPLICATES_KIS_ACTUAL")
            if reasons: issues.append({"table": table, "reasons": reasons, "row": row})
    return {"generated_at": datetime.now(timezone.utc).isoformat(), "orders": len(orders), "fills": len(fills),
            "issue_count": len(issues), "issues": issues}


def apply_integrity_plan(engine, trade_date: str, result: dict, *, actual_fills: list[dict] | None = None,
                         authoritative_positions: list[dict] | None = None) -> dict:
    """Quarantine and delete only identified rows in one transaction."""
    from sqlalchemy import text
    counts = {"quarantined_rows": 0, "corrected_rows": 0,
              "deleted_synthetic_duplicates": 0, "closed_stale_positions": 0,
              "recalculated_realized_pnl_rows": 0, "repaired_lifecycle_rows": 0,
              "regenerated_reports": 0,
              "row_ids": []}
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE IF NOT EXISTS us_trade_integrity_quarantine (
          id bigserial PRIMARY KEY, quarantined_at timestamptz NOT NULL DEFAULT now(),
          source_table text NOT NULL, reason text NOT NULL, original_row jsonb NOT NULL)"""))
        for issue in result.get("issues", []):
            table = issue.get("table")
            row = issue.get("row") or {}
            row_id = row.get("id")
            if table not in {"us_orders", "us_fills"} or row_id is None:
                raise RuntimeError(f"repair requires primary-key evidence: {table} id={row_id}")
            reason = ",".join(issue.get("reasons") or [])
            conn.execute(text("""INSERT INTO us_trade_integrity_quarantine(source_table,reason,original_row)
                VALUES (:table,:reason,CAST(:row AS jsonb))"""),
                {"table": table, "reason": reason, "row": json.dumps(row, default=str)})
            deleted = conn.execute(text(f"DELETE FROM {table} WHERE id=:id"), {"id": row_id})
            if int(deleted.rowcount or 0) != 1:
                raise RuntimeError(f"concurrent repair conflict: {table} id={row_id}")
            counts["quarantined_rows"] += 1
            counts["row_ids"].append({"table": table, "id": row_id})
            if "SYNTHETIC_DUPLICATES_KIS_ACTUAL" in issue.get("reasons", []):
                counts["deleted_synthetic_duplicates"] += 1
        grouped_actual = {}
        for fill in actual_fills or []:
            order_no = str(fill.get("order_no") or "")
            symbol = str(fill.get("symbol") or "").upper()
            side = str(fill.get("side") or "").upper()
            if not order_no or not symbol or not side:
                continue
            qty = int(fill.get("cumulative_filled_qty") or fill.get("qty") or fill.get("filled_qty") or 0)
            price = float(fill.get("avg_price_usd") or fill.get("price_usd") or fill.get("price") or fill.get("avg_price") or 0)
            observed_at = str(fill.get("observed_at") or fill.get("order_timestamp") or fill.get("filled_at") or "")
            key = (trade_date, order_no, symbol, side)
            current = grouped_actual.get(key)
            if current and qty < int(current.get("qty") or 0) and observed_at > str(current.get("observed_at") or ""):
                counts["row_ids"].append({"table":"us_fills","order_no":order_no,"reason":"REPAIR_CUMULATIVE_SNAPSHOT_CONFLICT"})
                continue
            if current is None or qty > int(current.get("qty") or 0) or observed_at >= str(current.get("observed_at") or ""):
                grouped_actual[key] = {"qty": qty, "avg_price": price, "sample": fill, "observed_at": observed_at}
        for (td, order_no, symbol, side), agg in grouped_actual.items():
            qty = int(agg["qty"] or 0)
            avg_price = float(agg.get("avg_price") or 0.0)
            updated = conn.execute(text("""UPDATE us_orders SET qty_filled=:qty, avg_price_usd=:avg_price,
              status=CASE WHEN :qty < qty_requested THEN 'PARTIALLY_FILLED' ELSE 'FILLED' END,
              meta=COALESCE(meta,'{}'::jsonb)||jsonb_build_object('remaining_qty',GREATEST(qty_requested-:qty,0),'repair_actual_cumulative_qty',:qty)
              WHERE trade_date=:td AND order_no=:order_no AND symbol=:symbol AND side=:side"""),
              {"qty":qty,"avg_price":avg_price,"td":td,"order_no":order_no,"symbol":symbol,"side":side})
            if int(updated.rowcount or 0) == 0:
                sample = agg.get("sample") or {}
                cok = str(sample.get("client_order_key") or "").strip()
                requested_qty = int(sample.get("requested_qty") or sample.get("order_qty") or qty or 0)
                if cok:
                    status = "PARTIALLY_FILLED" if qty < requested_qty else "FILLED"
                    conn.execute(text("""INSERT INTO us_orders(trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,avg_price_usd,order_no,status,meta)
                      VALUES(:td,:cok,:symbol,'NASDAQ',:side,:requested,:qty,:avg_price,:order_no,:status,jsonb_build_object('repair_reconstructed',true,'remaining_qty',GREATEST(:requested-:qty,0)))"""),
                      {"td":td,"cok":cok,"symbol":symbol,"side":side,"requested":requested_qty,"qty":qty,"avg_price":avg_price,"order_no":order_no,"status":status})
                    counts["corrected_rows"] += 1
                else:
                    counts["row_ids"].append({"table":"us_orders","order_no":order_no,"reason":"REPAIR_ORDER_IDENTITY_UNRESOLVED"})
            else:
                counts["corrected_rows"] += int(updated.rowcount or 0)
            from trader.us.db.repos import canonical_kis_order_cumulative_key
            idem = canonical_kis_order_cumulative_key(trade_date=td, order_no=order_no, symbol=symbol, side=side)
            sample = agg.get("sample") or {}
            requested_qty = int(sample.get("requested_qty") or sample.get("order_qty") or qty or 0)
            remaining_qty = int(sample.get("remaining_qty") or max(requested_qty - qty, 0))
            observed_at = str(agg.get("observed_at") or "")
            conn.execute(text("""INSERT INTO us_fills(trade_date,symbol,exchange,side,qty,price_usd,order_no,client_order_key,filled_at,meta,fill_idempotency_key)
              SELECT :td,:symbol,'NASDAQ',:side,:qty,:price,:order_no,client_order_key,:filled_at,
                     jsonb_build_object('is_synthetic',false,'fill_evidence_type','KIS_ORDER_CUMULATIVE_ACTUAL','source_endpoint','KIS_INQUIRE_CCNL','cumulative_filled_qty',:qty,'requested_qty',:requested,'remaining_qty',:remaining,'observed_at',:filled_at),:idem
              FROM us_orders WHERE trade_date=:td AND order_no=:order_no AND symbol=:symbol AND side=:side LIMIT 1
              ON CONFLICT (fill_idempotency_key) DO UPDATE SET qty=EXCLUDED.qty, price_usd=EXCLUDED.price_usd, filled_at=EXCLUDED.filled_at, client_order_key=EXCLUDED.client_order_key, meta=us_fills.meta||EXCLUDED.meta
              WHERE COALESCE((EXCLUDED.meta->>'cumulative_filled_qty')::integer,EXCLUDED.qty) >= COALESCE((us_fills.meta->>'cumulative_filled_qty')::integer,us_fills.qty)"""),
              {"td":td,"symbol":symbol,"side":side,"qty":qty,"price":avg_price,"order_no":order_no,"filled_at":observed_at,"requested":requested_qty,"remaining":remaining_qty,"idem":idem})
            conn.execute(text("""UPDATE us_fills SET meta=COALESCE(meta,'{}'::jsonb)||jsonb_build_object('accounting_active',false,'superseded_by_kis_actual',true)
              WHERE trade_date=:td AND order_no=:order_no AND symbol=:symbol AND side=:side
              AND COALESCE((meta->>'is_synthetic')::boolean,(meta->>'synthetic')::boolean,(meta->>'synthetic_fill')::boolean,false)"""),
              {"td":td,"order_no":order_no,"symbol":symbol,"side":side})
            pnl = conn.execute(text("""UPDATE us_fills SET meta=COALESCE(meta,'{}'::jsonb)||
              jsonb_build_object('realized_pnl_usd',(price_usd-NULLIF((meta->>'cost_basis_price_usd')::numeric,0))*qty)
              WHERE trade_date=:td AND order_no=:order_no AND side='SELL' AND meta ? 'cost_basis_price_usd'"""),
              {"td":td,"order_no":order_no})
            counts["recalculated_realized_pnl_rows"] += int(pnl.rowcount or 0)
        if authoritative_positions is not None:
            symbols=[str(p.get("symbol") or "").upper() for p in authoritative_positions]
            closed=conn.execute(text("""UPDATE us_positions SET qty=0,
              meta=COALESCE(meta,'{}'::jsonb)||'{"position_status":"CLOSED_BY_INTEGRITY_REPAIR"}'::jsonb
              WHERE as_of=:td AND qty>0 AND NOT (UPPER(symbol)=ANY(:symbols))"""),
              {"td":trade_date,"symbols":symbols or ["__NONE__"]})
            counts["closed_stale_positions"] += int(closed.rowcount or 0)
            try:
                from trader.us.position_lifecycle_state import reconcile_us_position_lifecycles
                lifecycle_map = reconcile_us_position_lifecycles(positions=authoritative_positions, trade_date=trade_date, now=datetime.now(timezone.utc), authoritative=True)
                for symbol, state in lifecycle_map.items():
                    lifecycle=conn.execute(text("""UPDATE us_position_risk_state SET state=COALESCE(state,'{}'::jsonb)||jsonb_build_object('lifecycle',CAST(:lifecycle AS jsonb))
                      WHERE trade_date=:td AND symbol=:symbol"""), {"td":trade_date,"symbol":symbol,"lifecycle":json.dumps(state, default=str)})
                    counts["repaired_lifecycle_rows"] += int(lifecycle.rowcount or 0)
            except Exception as exc:
                counts["lifecycle_repair_failed"] = True
                counts["lifecycle_repair_error"] = str(exc)
                raise
    counts["destructive_mutations"] = sum(counts[k] for k in ("quarantined_rows","corrected_rows","deleted_synthetic_duplicates","closed_stale_positions","recalculated_realized_pnl_rows","repaired_lifecycle_rows"))
    return counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--env", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.apply and args.env.lower() not in {"practice", "vps", "paper"}:
        parser.error("automatic repair is forbidden outside KIS practice")
    from trader.us.db.repos import load_today_fills, load_us_daily_orders_for_report, _get_engine_or_none
    engine = _get_engine_or_none()
    if args.apply and engine is None:
        parser.error("--apply requires a configured practice database")
    if engine is not None:
        from sqlalchemy import text
        with engine.begin() as conn:
            orders = [dict(r) for r in conn.execute(text("SELECT * FROM us_orders WHERE trade_date=:td"), {"td": args.trade_date}).mappings()]
            fills = [dict(r) for r in conn.execute(text("SELECT * FROM us_fills WHERE trade_date=:td"), {"td": args.trade_date}).mappings()]
    else:
        orders = load_us_daily_orders_for_report(args.trade_date) or []
        fills = load_today_fills(args.trade_date) or []
    result = audit(orders, fills)
    out = Path("reports/us_integrity")
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{args.trade_date}-before.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    quarantine = {"trade_date": args.trade_date, "applied": bool(args.apply), "rows": result["issues"]}
    (out / f"{args.trade_date}-quarantine.json").write_text(json.dumps(quarantine, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    actual_fills: list[dict] = []
    authoritative_positions = None
    if args.apply:
        from trader.us.data_provider import USDataProvider
        from trader.us.execution.fills import get_fills_today
        provider = USDataProvider(offline=False)
        fill_result = get_fills_today(provider=provider, trade_date=args.trade_date)
        if fill_result.get("status") != "OK": raise RuntimeError("KIS actual fills unavailable; refusing repair")
        actual_fills = fill_result.get("fills") or []
        balance = provider.get_balance(force_refresh=True)
        authoritative_positions = balance.get("positions") if balance.get("balance_parse_status", "OK") == "OK" else None
        if authoritative_positions is None: raise RuntimeError("KIS authoritative balance unavailable; refusing repair")
    mutations = apply_integrity_plan(engine, args.trade_date, result, actual_fills=actual_fills,
                                     authoritative_positions=authoritative_positions) if args.apply else {
        "destructive_mutations": 0, "quarantined_rows": 0, "corrected_rows": 0,
        "deleted_synthetic_duplicates": 0, "closed_stale_positions": 0,
        "recalculated_realized_pnl_rows": 0, "repaired_lifecycle_rows": 0,
        "regenerated_reports": 0, "row_ids": []}
    if args.apply:
        from trader.us.runner.daily_report_runner import run_daily_report
        report_result=run_daily_report(env="practice",session="close",trade_date=args.trade_date,
            final_balance=balance,final_positions=authoritative_positions,kis_fills=actual_fills)
        if report_result.get("status") not in {"OK","OK_WITH_WARNINGS"}: raise RuntimeError("daily report regeneration failed")
        mutations["regenerated_reports"] = 1
    after = {**result, "mode": "apply" if args.apply else "dry_run", **mutations}
    (out / f"{args.trade_date}-after.json").write_text(json.dumps(after, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(json.dumps({"trade_date": args.trade_date, "issues": result["issue_count"], "applied": args.apply}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
