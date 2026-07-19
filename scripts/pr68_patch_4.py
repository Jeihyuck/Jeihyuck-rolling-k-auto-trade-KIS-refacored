#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    (ROOT / path).write_text(text, encoding="utf-8")


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one literal match, found {count}: {old[:120]!r}")
    write(path, text.replace(old, new, 1))


def replace_regex(path: str, pattern: str, new: str) -> None:
    text = read(path)
    updated, count = re.subn(pattern, new, text, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError(f"{path}: expected one regex match, found {count}: {pattern[:120]!r}")
    write(path, updated)


# Journal replay: regression/contract evidence must never reach fill confirmation.
replace_once(
    "trader/us/execution/order_journal.py",
    '''            if broker_fill and int(broker_fill.get("filled_qty") or 0) > 0:
                if str(broker_fill.get("symbol") or symbol).upper()!=symbol or str(broker_fill.get("side") or side).upper()!=side:
''',
    '''            broker_fill_status = str(
                (broker_fill or {}).get("evidence_status")
                or (broker_fill or {}).get("status")
                or "OK"
            ).upper()
            if broker_fill_status in {
                "EVIDENCE_QUANTITY_REGRESSION",
                "EVIDENCE_QUANTITY_CONFLICT",
                "EVIDENCE_QUANTITY_OVERFLOW",
                "FILL_ACCOUNTING_INVARIANT_FAILED",
                "CONTRACT_ERROR",
                "RECONCILE_UPDATE_FAILED",
            }:
                counts["failed_count"] += 1
                unresolved_symbol_sides.append([symbol, side])
                append_order_event(
                    "JOURNAL_REPLAY_FAILED",
                    ack,
                    broker_order_no=order_no,
                    broker_status=broker_fill_status,
                    raw_response=broker_fill,
                )
                continue
            if broker_fill and int(broker_fill.get("filled_qty") or 0) > 0:
                if str(broker_fill.get("symbol") or symbol).upper()!=symbol or str(broker_fill.get("side") or side).upper()!=side:
''',
)

# Repair overflow guard plus in-transaction final accounting invariant.
replace_once(
    "scripts/repair_us_trade_integrity.py",
    '''def _is_synthetic(row: dict) -> bool:
    meta = _meta(row)
    return bool(meta.get("is_synthetic") or meta.get("synthetic") or meta.get("synthetic_fill")
                or meta.get("fill_evidence_type") in {"BALANCE_DELTA_SYNTHETIC", "LEGACY_SYNTHETIC"})


def audit''',
    '''def _is_synthetic(row: dict) -> bool:
    meta = _meta(row)
    return bool(meta.get("is_synthetic") or meta.get("synthetic") or meta.get("synthetic_fill")
                or meta.get("fill_evidence_type") in {"BALANCE_DELTA_SYNTHETIC", "LEGACY_SYNTHETIC"})


def validate_repair_cumulative(*, qty: int, requested_qty: int) -> dict:
    qty = int(qty or 0)
    requested_qty = int(requested_qty or 0)
    if qty < 0:
        return {"status": "EVIDENCE_QUANTITY_INVALID", "qty": qty, "requested_qty": requested_qty}
    if requested_qty > 0 and qty > requested_qty:
        return {"status": "EVIDENCE_QUANTITY_OVERFLOW", "qty": qty, "requested_qty": requested_qty}
    return {"status": "OK", "qty": qty, "requested_qty": requested_qty}


def audit''',
)
replace_once(
    "scripts/repair_us_trade_integrity.py",
    '''            qty = int(agg["qty"] or 0)
            avg_price = float(agg.get("avg_price") or 0.0)
            updated = conn.execute(text("""UPDATE us_orders SET qty_filled=:qty, avg_price_usd=:avg_price,
''',
    '''            qty = int(agg["qty"] or 0)
            avg_price = float(agg.get("avg_price") or 0.0)
            existing_order = conn.execute(
                text("""SELECT qty_requested FROM us_orders
                        WHERE trade_date=:td AND order_no=:order_no AND symbol=:symbol AND side=:side
                        FOR UPDATE"""),
                {"td": td, "order_no": order_no, "symbol": symbol, "side": side},
            ).mappings().first()
            existing_requested = int((existing_order or {}).get("qty_requested") or 0)
            validation = validate_repair_cumulative(qty=qty, requested_qty=existing_requested)
            if validation["status"] != "OK":
                counts["row_ids"].append({
                    "table": "us_orders",
                    "order_no": order_no,
                    "reason": validation["status"],
                    "observed_cumulative": qty,
                    "requested_qty": existing_requested,
                })
                continue
            updated = conn.execute(text("""UPDATE us_orders SET qty_filled=:qty, avg_price_usd=:avg_price,
''',
)
replace_once(
    "scripts/repair_us_trade_integrity.py",
    '''            conn.execute(text("""UPDATE us_fills SET meta=COALESCE(meta,'{}'::jsonb)||jsonb_build_object('accounting_active',false,'superseded_by_kis_actual',true)
              WHERE trade_date=:td AND order_no=:order_no AND symbol=:symbol AND side=:side
              AND COALESCE((meta->>'is_synthetic')::boolean,(meta->>'synthetic')::boolean,(meta->>'synthetic_fill')::boolean,false)"""),
              {"td":td,"order_no":order_no,"symbol":symbol,"side":side})
            pnl = conn.execute(text("""UPDATE us_fills SET meta=COALESCE(meta,'{}'::jsonb)||
''',
    '''            conn.execute(text("""UPDATE us_fills SET meta=COALESCE(meta,'{}'::jsonb)||jsonb_build_object('accounting_active',false,'superseded_by_kis_actual',true)
              WHERE trade_date=:td AND order_no=:order_no AND symbol=:symbol AND side=:side
              AND COALESCE((meta->>'is_synthetic')::boolean,(meta->>'synthetic')::boolean,(meta->>'synthetic_fill')::boolean,false)"""),
              {"td":td,"order_no":order_no,"symbol":symbol,"side":side})
            active_rows = conn.execute(text("""SELECT qty, meta FROM us_fills
              WHERE trade_date=:td AND order_no=:order_no AND symbol=:symbol AND side=:side
              AND COALESCE((meta->>'accounting_active')::boolean,true)"""),
              {"td":td,"order_no":order_no,"symbol":symbol,"side":side}).mappings().all()
            execution_qty = 0
            cumulative_qty = 0
            synthetic_qty = 0
            for active_row in active_rows:
                active_meta = _meta(active_row)
                active_qty = int(active_row.get("qty") or 0)
                evidence_type = str(active_meta.get("fill_evidence_type") or "")
                if _is_synthetic(active_row):
                    synthetic_qty = max(synthetic_qty, int(active_meta.get("cumulative_filled_qty") or active_qty or 0))
                elif evidence_type in {"KIS_EXECUTION_ACTUAL", "KIS_ACTUAL"}:
                    execution_qty += active_qty
                else:
                    cumulative_qty = max(cumulative_qty, int(active_meta.get("cumulative_filled_qty") or active_qty or 0))
            active_total = execution_qty if execution_qty > 0 else cumulative_qty if cumulative_qty > 0 else synthetic_qty
            if active_total != qty:
                raise RuntimeError(
                    f"repair fill accounting invariant failed order_no={order_no} order_qty={qty} active_fill_qty={active_total}"
                )
            pnl = conn.execute(text("""UPDATE us_fills SET meta=COALESCE(meta,'{}'::jsonb)||
''',
)
