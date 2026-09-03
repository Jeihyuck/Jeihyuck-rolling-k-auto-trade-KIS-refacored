#!/usr/bin/env python3
"""Audit/apply KR PB1 provenance from an exported broker/history JSON bundle."""
import argparse
import json
import logging
from pathlib import Path

from trader.kr.position_provenance import Confidence, audit_position


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--apply-db", action="store_true")
    parser.add_argument("--confirm", action="store_true", help="required with --apply-db")
    parser.add_argument("--fresh-broker-input", help="second, immediately refreshed KIS positions JSON")
    parser.add_argument("--input", help="JSON: positions, fills, trade_date (omit for an empty connectivity-safe audit)")
    parser.add_argument("--output", help="write confirmed non-destructive update plan")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    payload = json.loads(Path(args.input).read_text()) if args.input else {"positions": [], "fills": []}
    audits = [audit_position(p, payload.get("fills", []), trade_date=payload.get("trade_date"))
              for p in payload.get("positions", [])]
    confirmed = []
    counts = {confidence.value: 0 for confidence in Confidence}
    for result in audits:
        tag = result.confidence.value
        counts[tag] += 1
        logging.info("[KR_POSITION_PROVENANCE][AUDIT] symbol=%s current=%s@%s reconstructed=%s@%s confidence=%s reason=%s original_buy_id=%s updates=%s",
                     result.symbol, result.current_qty, result.current_avg, result.reconstructed_qty,
                     result.reconstructed_avg, tag, result.reason, result.original_buy_id, result.updates or {})
        logging.info("[KR_POSITION_PROVENANCE][%s] symbol=%s", tag, result.symbol)
        if result.confidence is Confidence.CONFIRMED:
            source = next(p for p in payload["positions"] if str(p.get("symbol") or p.get("code")) == result.symbol)
            confirmed.append({"symbol": result.symbol, "code": result.symbol,
                              "env": source.get("env"), "strategy": source.get("strategy") or "PB1",
                              "sid": source.get("sid", 1), "mode": source.get("mode", 1),
                              "current_qty": result.current_qty, "current_avg": result.current_avg,
                              "original_buy_id": result.original_buy_id, "updates": result.updates})
    if args.apply:
        if not args.output:
            parser.error("--apply requires --output; direct live DB writes are intentionally unsupported")
        Path(args.output).write_text(json.dumps(confirmed, ensure_ascii=False, indent=2, default=str))
        for row in confirmed:
            logging.info("[KR_POSITION_PROVENANCE][APPLIED] symbol=%s target=update-plan", row["symbol"])
    if args.apply_db:
        if not args.confirm or not args.fresh_broker_input:
            parser.error("--apply-db requires --confirm and --fresh-broker-input")
        fresh = json.loads(Path(args.fresh_broker_input).read_text())
        fresh_by_symbol = {str(p.get("symbol") or p.get("code")): p for p in fresh.get("positions", [])}
        for repair in confirmed:
            current = fresh_by_symbol.get(repair["symbol"])
            if (not current or int(current.get("qty") or 0) != repair["current_qty"]
                    or float(current.get("average_price") or current.get("avg_price") or 0) != repair["current_avg"]
                    or str(current.get("env") or "") != str(repair["env"] or "")):
                raise SystemExit(f"STALE_AUDIT_POSITION_CHANGED:{repair['symbol']}")
        from trader.db.engine import get_engine
        from trader.db.repos import PositionsRepo
        applied = PositionsRepo(get_engine()).apply_confirmed_provenance_repairs(confirmed)
        logging.info("[KR_POSITION_PROVENANCE][APPLIED] rows=%s transaction=COMMITTED", applied)
    logging.info("[KR_POSITION_PROVENANCE][SUMMARY] %s", counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
