"""PR249 quantitative pre-merge latency proof gate.

Do not promote a PR because a synthetic demo passed. Input must contain
versioned, same-workload *measured* ticks and timestamped broker submit events.

Example:
 {"revision":"before","workload_id":"kr-30-10-open5","tick_id":"a",
  "tick_elapsed_sec":321.2,"first_protective_sell_submit_sec":53.1,
  "kind":"measured","git_sha":"<real pre-PR SHA>"}
 {"revision":"after", ...,"kind":"measured","git_sha":"<PR head SHA>"}
"""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path


def _p95(values: list[float]) -> float:
    values = sorted(values)
    if not values:
        raise ValueError("PR249_LATENCY_NO_SAMPLES")
    return values[math.ceil(0.95 * len(values)) - 1]


def evaluate_latency(records: list[dict], *, min_ticks: int = 20, min_sells: int = 10) -> dict:
    """Fail closed on missing/mismatched workload, unmatched SHA and fake runs."""
    groups = defaultdict(list)
    for row in records:
        rev = str(row.get("revision") or "")
        if rev not in {"before", "after"}:
            raise ValueError("PR249_LATENCY_INVALID_REVISION")
        if row.get("kind") != "measured" or not str(row.get("git_sha") or "").strip():
            raise ValueError("PR249_LATENCY_UNPROVEN_SAMPLE")
        if rev == "after":
            # A fast crash is not a successful fast trade tick. Protect the
            # acceptance gate from a speed-up achieved by skipping work.
            status = str(row.get("result_status") or "").upper()
            if not status or status in {
                "TIMEOUT_OR_FAILURE", "RECOVERABLE_DB_TIMEOUT",
                "FATAL_RUNTIME", "FATAL_POSTPROCESS", "UNKNOWN",
                "PARTIAL_SUCCESS_POSTPROCESS_FAILED", "DEGRADED_BUDGET",
            } or status.startswith(("FATAL", "ERROR", "SKIP_KIS")):
                raise ValueError("PR249_LATENCY_FAILED_TICK_NOT_ACCEPTABLE")
        wid = str(row.get("workload_id") or "").strip()
        tick = str(row.get("tick_id") or "").strip()
        if not wid or not tick:
            raise ValueError("PR249_LATENCY_MISSING_SCOPING")
        sec = float(row.get("tick_elapsed_sec") or 0)
        if not math.isfinite(sec) or sec <= 0:
            raise ValueError("PR249_LATENCY_BAD_TICK_VALUE")
        if row.get("first_protective_sell_submit_sec") is not None:
            submit = float(row["first_protective_sell_submit_sec"])
            if not math.isfinite(submit) or submit < 0 or submit > sec:
                raise ValueError("PR249_LATENCY_BAD_SELL_VALUE")
        groups[(wid, rev)].append(row)
    scenarios = sorted({k[0] for k in groups})
    if not scenarios:
        raise ValueError("PR249_LATENCY_NO_SAMPLES")
    assessments = []
    for wid in scenarios:
        before = groups[(wid, "before")]
        after = groups[(wid, "after")]
        if len(before) < min_ticks or len(after) < min_ticks:
            raise ValueError(f"PR249_LATENCY_INSUFFICIENT_TICKS:{wid}")
        if len({x["tick_id"] for x in before}) != len(before) or len({x["tick_id"] for x in after}) != len(after):
            raise ValueError(f"PR249_LATENCY_DUPLICATE_TICK_ID:{wid}")
        before_sha = {str(x["git_sha"]) for x in before}
        after_sha = {str(x["git_sha"]) for x in after}
        if len(before_sha) != 1 or len(after_sha) != 1 or before_sha == after_sha:
            raise ValueError(f"PR249_LATENCY_SHA_NOT_COMPARABLE:{wid}")
        before_sell = [float(x["first_protective_sell_submit_sec"]) for x in before if x.get("first_protective_sell_submit_sec") is not None]
        after_sell = [float(x["first_protective_sell_submit_sec"]) for x in after if x.get("first_protective_sell_submit_sec") is not None]
        if len(before_sell) < min_sells or len(after_sell) < min_sells:
            raise ValueError(f"PR249_LATENCY_INSUFFICIENT_SELL_PROOFS:{wid}")
        p95_tick_before = _p95([float(x["tick_elapsed_sec"]) for x in before])
        p95_tick_after = _p95([float(x["tick_elapsed_sec"]) for x in after])
        p95_sell_before = _p95(before_sell)
        p95_sell_after = _p95(after_sell)
        assessments.append({
            "workload_id": wid, "before_git_sha": next(iter(before_sha)),
            "after_git_sha": next(iter(after_sha)), "before_ticks": len(before),
            "after_ticks": len(after), "before_sell_samples": len(before_sell),
            "after_sell_samples": len(after_sell),
            "p95_tick_before_sec": p95_tick_before,
            "p95_tick_after_sec": p95_tick_after,
            "p95_sell_before_sec": p95_sell_before,
            "p95_sell_after_sec": p95_sell_after,
            "tick_limit_pass": p95_tick_after <= 180.0,
            "sell_limit_pass": p95_sell_after <= 60.0,
            "no_tick_regression": p95_tick_after < p95_tick_before,
            "no_sell_regression": p95_sell_after <= p95_sell_before,
        })
    passed = all(x["tick_limit_pass"] and x["sell_limit_pass"] and x["no_tick_regression"] and x["no_sell_regression"] for x in assessments)
    return {"result": "PASS" if passed else "FAIL", "scenarios": assessments}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("jsonl", type=Path)
    p.add_argument("--min-ticks", type=int, default=20)
    p.add_argument("--min-sells", type=int, default=10)
    args = p.parse_args()
    try:
        rows = [json.loads(line) for line in args.jsonl.read_text(encoding="utf-8").splitlines() if line.strip()]
        outcome = evaluate_latency(rows, min_ticks=args.min_ticks, min_sells=args.min_sells)
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"result": "BLOCK", "reason": str(exc)}))
        return 2
    print(json.dumps(outcome, indent=2, ensure_ascii=False))
    return 0 if outcome["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
