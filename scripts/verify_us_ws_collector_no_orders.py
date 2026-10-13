#!/usr/bin/env python3
"""Read-only KIS practice WebSocket collector smoke check on the deployment WSL.

It neither constructs an order router nor submits, cancels or acknowledges orders.
Run only when the operator intends to consume practice market-data quota.
Example:
  US_WS_COLLECTOR_ENABLED=1 DRY_RUN=1 US_KIS_ORDER_ALLOWED=0 \\
    .venv/bin/python scripts/verify_us_ws_collector_no_orders.py \\
    --symbols MSFT:NASDAQ,KO:NYSE --seconds 25
"""
from __future__ import annotations

import argparse
import json
import os
import time
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def run(*, symbols: list[tuple[str, str]], seconds: float) -> dict:
    # Fail-closed: this process and its collector cannot submit orders.
    os.environ["DRY_RUN"] = "1"
    os.environ["US_KIS_ORDER_ALLOWED"] = "0"
    os.environ["KIS_ENV"] = "practice"
    os.environ["US_WS_COLLECTOR_ENABLED"] = "1"
    from trader.marketdata.us_ws_collector import (
        _send_request, ensure_started, get_quote,
    )

    launch = ensure_started(env="practice", offline=False)
    start = time.monotonic()
    outcomes = {sym: {"exchange": exch, "hit": 0, "miss": 0,
                      "generations": set(), "max_age_sec": None}
                for sym, exch in symbols}
    failures: list[str] = []
    generation = None
    while time.monotonic() - start < seconds:
        health = None
        try:
            health = _send_request("practice", {"cmd": "HEALTH"}, timeout_sec=0.15)
        except (OSError, ValueError, RuntimeError) as exc:
            failures.append(type(exc).__name__)
        if health and health.get("status") == "OK":
            generation = health.get("generation")
        for sym, exch in symbols:
            quote = get_quote(sym, exch, env="practice", max_age_sec=15, timeout_sec=0.2)
            row = outcomes[sym]
            if quote:
                row["hit"] += 1
                row["generations"].add(str(quote["collector_generation"]))
                age = float(quote.get("age_sec") or time.time() - float(quote["received_at"]))
                row["max_age_sec"] = max(row["max_age_sec"] or 0, age)
            else:
                row["miss"] += 1
        time.sleep(0.35)
    try:
        commit_sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, timeout=2,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        commit_sha = ""
    result = {
        "evidence_at_utc": datetime.now(timezone.utc).isoformat(),
        "commit_sha": commit_sha,
        "mode": "READ_ONLY_PRACTICE_MARKET_DATA",
        "no_order_submit": True,
        "duration_sec": round(time.monotonic() - start, 3),
        "collector_generation": generation,
        "collector_pid_requested": launch.pid if launch is not None else None,
        "symbols": {
            k: {**v, "generations": sorted(v["generations"]),
                "max_age_sec": round(v["max_age_sec"], 3)
                if v["max_age_sec"] is not None else None}
            for k, v in outcomes.items()
        },
        "ipc_failures": failures[:10],
        "verified_fresh_quote_for_all": all(
            v["hit"] > 0 for v in outcomes.values()
        ),
    }
    return result


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", default="MSFT:NASDAQ,KO:NYSE",
                   help="Comma-separated TICKER:EXCHANGE pairs; select held symbols")
    p.add_argument("--seconds", type=float, default=20)
    p.add_argument("--output", type=Path, default=None,
                   help="Optional JSON evidence output, e.g. reports/us_daily/<date>/ws_collector_smoke.json")
    args = p.parse_args()
    pairs = []
    for field in args.symbols.split(","):
        sym, sep, exch = field.strip().upper().partition(":")
        if not sep or not sym or exch not in {"NASDAQ", "NYSE", "AMEX"}:
            p.error(f"Invalid symbol/exchange pair: {field!r}")
        pairs.append((sym, exch))
    result = run(symbols=pairs, seconds=max(2.0, min(args.seconds, 120.0)))
    evidence = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(evidence + "\n", encoding="utf-8")
    print(evidence)
    # Quoting can legitimately be absent outside US market hours. Fail closed;
    # never describe an unreceived quote as a verified collector pass.
    return 0 if result["verified_fresh_quote_for_all"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
