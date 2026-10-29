#!/usr/bin/env python3
"""Offline US_STANDARD opening-entry replay directly from read-only observer DB.

No broker connection, no SQL writes, no assumption about missing bid/ask or
unknown actual fill. Uses only KIS-native minute bars previously persisted and
historical quote snapshots with their original capture timestamps.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from trader.us.entry_timing.opening_observer import load_bars, load_quote_samples
from trader.us.entry_timing.opportunity import evaluate_price_opportunity

NY = ZoneInfo("America/New_York")


def replay_observer_db(path: Path, *, trade_date: str, symbol: str,
                       style: str, prep: dict | None = None,
                       original_fill_price: float | None = None) -> dict:
    if not path.exists() or symbol.upper().strip() == "TQQQ":
        raise ValueError("missing_observer_db_or_nonstandard_symbol")
    # No implicit create, migration, INSERT, or network. Existing on-disk
    # snapshots are observational and must not be interpreted as fills.
    from urllib.parse import quote as url_quote
    uri = "file:" + url_quote(str(path.resolve()), safe="/") + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=0.2) as db:
        bars = load_bars(db, trade_date=trade_date, symbol=symbol, limit=450)
        quotes = load_quote_samples(db, trade_date=trade_date, symbol=symbol,
                                    limit=10000)
    decisions = []
    first_ready = None
    for quote in quotes:
        moment = datetime.fromtimestamp(quote["received_at"], NY)
        if moment.date().isoformat() != trade_date:
            continue
        # No bid/ask is ever inferred: missing genuine quote values => WAIT.
        decision = evaluate_price_opportunity(
            style=style, trade_date=trade_date, now_et=moment,
            verified_bars=bars,
            quote={
                "source": quote["source"], "last": quote["last"],
                "bid": quote["bid"], "ask": quote["ask"],
                "age_sec": 0.0,
                "collector_generation": quote["collector_generation"],
            },
            prep=prep or {},
        )
        event = {
            "observed_at_et": moment.isoformat(),
            "state": decision["state"], "reason": decision["reason"],
            "decision_price": decision.get("decision_price"),
            "maximum_entry_price": decision.get("max_entry_price"),
            "reference_reward_risk": decision.get("risk_reward_reference"),
            "evidence_sha256": decision.get("evidence_sha256"),
        }
        decisions.append(event)
        if first_ready is None and decision["state"] == "READY":
            first_ready = event
    result = {
        "mode": "OFFLINE_READ_ONLY_NO_KIS_NO_ORDER",
        "trade_date": trade_date, "symbol": symbol.upper(), "style": style,
        "verified_native_1m_rows": len(bars),
        "historical_ws_quote_samples": len(quotes),
        "observations": decisions,
        "first_ready": first_ready,
        "realized_pnl": None,
        "timing_profitability": "NOT_PROVEN",
    }
    if first_ready is None:
        result["forward_60m"] = {"status": "NO_READY"}
        return result
    moment = datetime.fromisoformat(first_ready["observed_at_et"])
    entry = float(first_ready["decision_price"])
    future = [
        row for row in bars
        if moment < datetime.fromisoformat(row["time_et"])
        <= moment + timedelta(minutes=60)
    ]
    if future:
        result["forward_60m"] = {
            "status": "OBSERVED_ORIGINAL_BARS_ONLY",
            "row_count": len(future),
            "mae_pct": round(100 * (min(b["low"] for b in future) / entry - 1), 4),
            "mfe_pct": round(100 * (max(b["high"] for b in future) / entry - 1), 4),
            "last_close_return_pct": round(100 * (future[-1]["close"] / entry - 1), 4),
        }
    else:
        result["forward_60m"] = {"status": "UNKNOWN_MISSING_FUTURE_BARS"}
    try:
        baseline = float(original_fill_price)
    except (TypeError, ValueError, OverflowError):
        baseline = None
    if baseline is not None and 0 < baseline < float("inf"):
        result["baseline_comparison"] = {
            "broker_fill_price_observed_elsewhere": baseline,
            "first_ready_quote_last": entry,
            "price_delta_pct": round(100 * (entry / baseline - 1), 4),
            "same_time_executable_fill_not_proven": True,
        }
    return result


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--observer-db", required=True, type=Path)
    p.add_argument("--trade-date", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--style", required=True)
    p.add_argument("--prep-json", type=Path)
    p.add_argument("--original-fill-price", type=float)
    p.add_argument("--output", type=Path)
    args = p.parse_args()
    prep = (json.loads(args.prep_json.read_text(encoding="utf-8"))
            if args.prep_json else {})
    result = replay_observer_db(
        args.observer_db, trade_date=args.trade_date,
        symbol=args.symbol, style=args.style, prep=prep,
        original_fill_price=args.original_fill_price,
    )
    payload = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0 if result["historical_ws_quote_samples"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
