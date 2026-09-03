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
    parser.add_argument("--input", help="JSON: positions, fills, trade_date (omit for an empty connectivity-safe audit)")
    parser.add_argument("--output", help="write confirmed non-destructive update plan")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    payload = json.loads(Path(args.input).read_text()) if args.input else {"positions": [], "fills": []}
    audits = [audit_position(p, payload.get("fills", []), trade_date=payload.get("trade_date"))
              for p in payload.get("positions", [])]
    confirmed = []
    for result in audits:
        tag = result.confidence.value
        logging.info("[KR_POSITION_PROVENANCE][AUDIT] symbol=%s current=%s@%s reconstructed=%s@%s confidence=%s reason=%s original_buy_id=%s updates=%s",
                     result.symbol, result.current_qty, result.current_avg, result.reconstructed_qty,
                     result.reconstructed_avg, tag, result.reason, result.original_buy_id, result.updates or {})
        logging.info("[KR_POSITION_PROVENANCE][%s] symbol=%s", tag, result.symbol)
        if result.confidence is Confidence.CONFIRMED:
            confirmed.append({"symbol": result.symbol, "original_buy_id": result.original_buy_id,
                              "updates": result.updates})
    if args.apply:
        if not args.output:
            parser.error("--apply requires --output; direct live DB writes are intentionally unsupported")
        Path(args.output).write_text(json.dumps(confirmed, ensure_ascii=False, indent=2, default=str))
        for row in confirmed:
            logging.info("[KR_POSITION_PROVENANCE][APPLIED] symbol=%s target=update-plan", row["symbol"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
