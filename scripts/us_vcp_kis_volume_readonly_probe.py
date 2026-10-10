"""Read-only KIS PRACTICE quote probe for the US Minervini/VCP merge gate.

Only GET /uapi/overseas-price/v1/quotations/price is used through KisUSClient.
Never sends an order or reads account balances. Does not enable the strategy.
Run during a regular US trading session, on the intended WSL environment,
with the existing .env loaded. All identifiers/passwords stay in that machine.

  set -a; source .env; set +a
  .venv/bin/python scripts/us_vcp_kis_volume_readonly_probe.py --symbol AAPL

A positive result is evidence of *fresh monotonic* KIS reported volume on
one US session. It cannot independently prove the vendor's "tvol" market
semantics, session reset or eligibility to merge an opt-in strategy.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime, time as day_time
from zoneinfo import ZoneInfo
from pathlib import Path

# Running `python scripts/this_file.py` does not otherwise add repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _positive(value: object) -> float | None:
    try:
        f = float(str(value).replace(",", ""))
        return f if math.isfinite(f) and f > 0 else None
    except (TypeError, ValueError):
        return None


def summarize_quotes(samples: list[dict], *, regular_session: bool) -> dict:
    """Pure acceptance check: no mocked/overnight volume is broker truth."""
    issues: list[str] = []
    if not regular_session:
        issues.append("outside_us_regular_session")
    if len(samples) < 2:
        issues.append("requires_two_independent_get_responses")
    previous_volume: float | None = None
    for i, sample in enumerate(samples):
        age = sample.get("age_sec")
        age = 0.0 if age == 0 else _positive(age)
        if sample.get("source") != "KIS_LIVE" or bool(sample.get("stale")):
            issues.append(f"sample_{i}_unverified_source")
        if age is None or age > 15:
            issues.append(f"sample_{i}_stale_timestamp")
        if _positive(sample.get("last")) is None:
            issues.append(f"sample_{i}_bad_price")
        volume = _positive(sample.get("tvol"))
        if volume is None:
            issues.append(f"sample_{i}_no_day_volume")
        elif previous_volume is not None and volume < previous_volume:
            issues.append(f"sample_{i}_volume_decreased_same_session")
        if volume is not None:
            previous_volume = volume
    return {
        "status": "OBSERVATIONS_PASS" if not issues else "INCONCLUSIVE",
        "sample_count": len(samples),
        "regular_us_session": regular_session,
        "reasons": issues,
        "samples": [{
            "price": x.get("last"),
            "reported_tvol": x.get("tvol"),
            "age_sec": x.get("age_sec"),
            "source": x.get("source"),
            "quote_quality": x.get("quality"),
            "latency_ms": x.get("latency_ms"),
            "observed_et": x.get("observed_et"),
        } for x in samples],
    }


def run_probe(symbol: str, exchange: str, *, samples: int, interval: float) -> dict:
    from trader.us.execution.kis_us_client import KisUSClient
    os.environ["KIS_ENV"] = "practice"
    os.environ["DRY_RUN"] = "1"
    os.environ["US_KIS_ORDER_ALLOWED"] = "0"
    os.environ["ALLOW_REAL_ORDER"] = "0"
    os.environ["US_KIS_PRICE_CACHE_TTL_SEC"] = "0"
    client = KisUSClient(env="practice", offline=False)
    et = ZoneInfo("America/New_York")
    observed: list[dict] = []
    session_days: set[str] = set()
    regular_flags: list[bool] = []
    for index in range(samples):
        now_et = datetime.now(et)
        regular = (now_et.weekday() < 5 and
                   day_time(9, 30) <= now_et.time() < day_time(16, 0))
        regular_flags.append(regular)
        session_days.add(now_et.date().isoformat())
        started = time.monotonic()
        raw = client.get_us_price(symbol, exchange)
        latency_ms = round((time.monotonic() - started) * 1000, 1)
        output = raw.get("output") if isinstance(raw, dict) else None
        output = output if isinstance(output, dict) else {}
        observed.append({
            "last": output.get("last"),
            "tvol": output.get("tvol"),
            "age_sec": raw.get("_quote_age_sec") if isinstance(raw, dict) else None,
            "source": raw.get("_quote_source") if isinstance(raw, dict) else None,
            "quality": raw.get("_quote_quality") if isinstance(raw, dict) else None,
            "stale": str(raw.get("_quote_quality") or "").upper()
                     not in {"FRESH", "FRESH_CACHE"},
            "latency_ms": latency_ms,
            "observed_et": now_et.isoformat(timespec="seconds"),
        })
        if index < samples - 1:
            time.sleep(interval)
    report = summarize_quotes(observed, regular_session=all(regular_flags) and len(session_days) == 1)
    report["symbol"] = symbol
    report["exchange"] = exchange
    report["environment"] = "practice"
    report["read_only"] = True
    report["requires_manual_vendor_semantics_confirmation"] = True
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only KIS US VCP volume preflight")
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--exchange", default="NASDAQ",
                        choices=["NASDAQ", "NYSE", "AMEX"])
    parser.add_argument("--samples", type=int, default=2)
    parser.add_argument("--interval", type=float, default=3)
    args = parser.parse_args(argv)
    if not args.symbol.isascii() or not args.symbol.replace(".", "").isalpha():
        parser.error("invalid ticker")
    if not 2 <= args.samples <= 4 or not 1 <= args.interval <= 10:
        parser.error("samples=2..4 and interval=1..10 required")
    if os.getenv("KIS_ENV", "practice").strip().lower() != "practice":
        parser.error("KIS_ENV must be practice; no live credential path")
    try:
        report = run_probe(args.symbol.upper(), args.exchange,
                           samples=args.samples, interval=args.interval)
    except Exception as exc:
        # Do not print vendor exception messages: they may contain sensitive data.
        print(json.dumps({"status": "INCONCLUSIVE", "read_only": True,
                          "reason_type": type(exc).__name__}))
        return 2
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    return 0 if report["status"] == "OBSERVATIONS_PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
