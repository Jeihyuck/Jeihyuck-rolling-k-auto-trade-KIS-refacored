#!/usr/bin/env python3
"""Read-only JSONL phase telemetry summarizer; never interprets missing as success.

Each input line: {"signal_to_intent": 0.1, "submit_to_ack": 0.4, ...}
Partial lines are allowed, but --require-stage4 fails unless >=20 actual
READY intervals were measured and none >=30s. Does not prove broker fills.
"""
from __future__ import annotations

import argparse
import json
import sys

from trader.us.execution.latency_evidence import latency_summary

def read_samples(path: str) -> tuple[list[dict], int]:
    samples: list[dict] = []
    bad = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                parsed = json.loads(line)
                if not isinstance(parsed, dict):
                    raise ValueError("not_object")
                samples.append(parsed)
            except (ValueError, TypeError):
                bad += 1
    return samples, bad

def analyze(path: str, require_stage4: bool = False) -> tuple[dict, bool]:
    samples, malformed = read_samples(path)
    result = latency_summary(samples)
    result["malformed_lines"] = malformed
    stage4 = result["phases"]["stage4_ready_interval"]
    result["stage4_evidence_status"] = (
        "PASS" if (stage4["count"] >= 20 and
                   stage4["stale_risk_count"] == 0 and
                   stage4["p95_sec"] is not None and stage4["p95_sec"] <= 25 and
                   stage4["p99_sec"] is not None and stage4["p99_sec"] <= 30 and
                   malformed == 0)
        else "NOT_PROVEN"
    )
    # No field here represents KIS ACK, fills, SELL-priority or CLOSE success.
    return result, (not require_stage4 or result["stage4_evidence_status"] == "PASS")

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("jsonl")
    p.add_argument("--require-stage4", action="store_true")
    args = p.parse_args(argv)
    try:
        report, ok = analyze(args.jsonl, args.require_stage4)
    except (OSError, UnicodeError) as exc:
        print(json.dumps({"status": "NOT_PROVEN", "error": type(exc).__name__}))
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if ok else 2

if __name__ == "__main__":
    sys.exit(main())
