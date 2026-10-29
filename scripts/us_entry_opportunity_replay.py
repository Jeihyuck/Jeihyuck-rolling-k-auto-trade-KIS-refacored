#!/usr/bin/env python3
"""Offline matched-time replay of actual KIS 1m bars and observed quote records.

Input JSON: {trade_date, style, prep, native_bars:[KIS raw rows],
             snapshots:[{at_et ISO, quote:{source,last,bid,ask,age_sec}}],
             original_fill_price?:number}.
This never calls KIS or invents missing quote/volume/spread/trades.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from trader.us.entry_timing.opportunity import (
    evaluate_price_opportunity, normalize_bars,
)

NY = ZoneInfo("America/New_York")


def replay(payload: dict) -> dict:
    trade_date = str(payload["trade_date"])
    style = str(payload["style"])
    raw = payload.get("native_bars") or []
    snapshots = payload.get("snapshots") or []
    decisions = []
    ready = None
    for sample in sorted(snapshots, key=lambda x: x["at_et"]):
        instant = datetime.fromisoformat(sample["at_et"]).astimezone(NY)
        bars = normalize_bars(raw, trade_date=trade_date, now_et=instant)
        decision = evaluate_price_opportunity(
            style=style, trade_date=trade_date, now_et=instant,
            verified_bars=bars, quote=sample.get("quote") or {},
            prep=payload.get("prep") or {},
        )
        decisions.append({
            "time_et": instant.isoformat(), "state": decision["state"],
            "reason": decision["reason"], "decision_price": decision["decision_price"],
            "max_entry_price": decision["max_entry_price"],
            "rr_reference": decision["risk_reward_reference"],
            "proof_sha256": decision["evidence_sha256"],
        })
        if ready is None and decision["state"] == "READY":
            ready = (instant, decision)
    result = {
        "trade_date": trade_date, "style": style, "replay_mode": "OFFLINE_NO_BROKER",
        "samples": len(decisions), "decisions": decisions,
        "first_ready": decisions[next(i for i, x in enumerate(decisions)
                                   if x["state"] == "READY")]
                       if ready else None,
        "timing_effectiveness": "NOT_ESTABLISHED",
    }
    if ready is None:
        return result
    # Baseline comparison is available from the observed READY quote even
    # when no later bars exist. Do not claim a fill or realized profit.
    instant, entry_decision = ready
    entry = float(entry_decision["decision_price"])
    baseline = payload.get("original_fill_price")
    if baseline is not None:
        try:
            baseline = float(baseline)
        except (TypeError, ValueError, OverflowError):
            baseline = None
        if baseline is not None and 0 < baseline < float("inf"):
            result["baseline_comparison"] = {
                "original_fill_price": baseline,
                "first_ready_quote_price": entry,
                "entry_price_delta_pct": round(100 * (entry / baseline - 1), 4),
                "missed_opportunity_risk": "UNMEASURED",
            }
    # A forward MAE/MFE is observational, not a realized trade or PnL.
    forward = normalize_bars(
        raw, trade_date=trade_date, now_et=instant + timedelta(minutes=61)
    )
    future = [b for b in forward
              if instant < datetime.fromisoformat(b["time_et"]) <=
              instant + timedelta(minutes=60)]
    if not future:
        result["forward_60m"] = {"status": "UNKNOWN_MISSING_FUTURE_BARS"}
        return result
    entry = float(entry_decision["decision_price"])
    result["forward_60m"] = {
        "status": "OBSERVED_BARS_ONLY",
        "bars": len(future),
        "mae_pct": round(100 * (min(b["low"] for b in future) / entry - 1), 4),
        "mfe_pct": round(100 * (max(b["high"] for b in future) / entry - 1), 4),
        "last_close_return_pct": round(100 * (future[-1]["close"] / entry - 1), 4),
        "trade_pnl": None, "fees_and_slippage": None,
    }
    return result


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("input_json", type=Path)
    p.add_argument("--output", type=Path)
    args = p.parse_args()
    result = replay(json.loads(args.input_json.read_text(encoding="utf-8")))
    output = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    print(output)
    return 0 if result["samples"] > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
