"""Convert actual KR tick telemetry into independently-scoped p95 evidence.

No generated/synthetic times, no missing timeout exclusions, no KIS order calls.
Reject partial log sessions, omitted timeout ticks, missing SHA or unresolved
SELL calls. The output can feed scripts/kr/pr249_latency_acceptance.py.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Iterable

_TOKEN = re.compile(r"([a-z_]+)=([^\s]+)")


def _fields(line: str) -> dict[str, str]:
    return dict(_TOKEN.findall(line))


def export_measured_ticks(lines: Iterable[str], *, revision: str, workload_id: str) -> list[dict]:
    if revision not in {"before", "after"} or not workload_id:
        raise ValueError("PR249_LATENCY_INVALID_SCOPE")
    starts = 0
    ends: list[dict[str, str]] = []
    submit: dict[str, list[dict]] = {}
    returned: set[tuple[str, str]] = set()
    for line in lines:
        if "[PB1][TICK][CALL_RUN_ONCE]" in line:
            starts += 1
        if "[KR_LATENCY][TICK_END]" in line:
            ends.append(_fields(line))
        if "[KR_LATENCY][SELL_SUBMIT_START]" in line:
            attrs = _fields(line)
            if attrs.get("tick_origin_source") != "RUNNER_FULL_TICK":
                raise ValueError("PR249_LATENCY_SELL_ORIGIN_NOT_FULL_TICK")
            if not attrs.get("run_id") or not attrs.get("order_id"):
                raise ValueError("PR249_LATENCY_SELL_IDENTITY_MISSING")
            submit.setdefault(attrs["run_id"], []).append(attrs)
        if "[KR_LATENCY][SELL_SUBMIT_RETURN]" in line:
            attrs = _fields(line)
            if attrs.get("ack_observed") == "1":
                returned.add((attrs.get("run_id") or "", attrs.get("order_id") or ""))
    if starts == 0 or not ends:
        raise ValueError("PR249_LATENCY_LOG_HAS_NO_MEASURED_TICKS")
    if len(ends) != starts:
        raise ValueError(f"PR249_LATENCY_MISSING_TICK_END:{starts}:{len(ends)}")
    by_tick_id: set[str] = set()
    rows: list[dict] = []
    for row in ends:
        sha = row.get("sha") or ""
        tick_run_id = row.get("tick_run_id") or ""
        if not sha or sha == "UNVERSIONED" or not tick_run_id or tick_run_id == "UNRESOLVED_TICK_RUN_ID":
            raise ValueError("PR249_LATENCY_UNVERSIONED_OR_UNSCOPED_TICK")
        tick_key = f"{row.get('run_id')}:{row.get('kind')}:{row.get('index')}:{tick_run_id}"
        if tick_key in by_tick_id:
            raise ValueError("PR249_LATENCY_DUPLICATE_TICK_ID")
        by_tick_id.add(tick_key)
        try:
            elapsed = float(row["total_elapsed_s"])
        except (KeyError, ValueError) as exc:
            raise ValueError("PR249_LATENCY_MISSING_TICK_DURATION") from exc
        item = {
            "revision": revision, "workload_id": workload_id, "tick_id": tick_key,
            "tick_elapsed_sec": elapsed, "kind": "measured", "git_sha": sha,
            "result_status": row.get("result_status") or "UNKNOWN",
            "trace_source": "KR_RUNNER_FULL_TICK_LOG",
        }
        candidate_sells = submit.get(tick_run_id, [])
        if candidate_sells:
            unacked = [(tick_run_id, v["order_id"]) for v in candidate_sells
                       if (tick_run_id, v["order_id"]) not in returned]
            if unacked:
                raise ValueError("PR249_LATENCY_UNRESOLVED_SELL_SUBMIT")
            try:
                item["first_protective_sell_submit_sec"] = min(
                    float(v["from_tick_start_s"]) for v in candidate_sells
                )
            except (KeyError, ValueError) as exc:
                raise ValueError("PR249_LATENCY_SELL_SUBMIT_DURATION_INVALID") from exc
        rows.append(item)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("log", type=Path)
    parser.add_argument("--revision", choices=("before", "after"), required=True)
    parser.add_argument("--workload-id", required=True)
    parser.add_argument("--jsonl", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = export_measured_ticks(
            args.log.read_text(encoding="utf-8", errors="replace").splitlines(),
            revision=args.revision, workload_id=args.workload_id,
        )
    except ValueError as exc:
        print(json.dumps({"result": "BLOCK", "reason": str(exc)}))
        return 2
    args.jsonl.write_text(
        "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in result),
        encoding="utf-8",
    )
    print(json.dumps({"result": "EXPORTED", "ticks": len(result),
                      "sell_proofs": sum("first_protective_sell_submit_sec" in i for i in result)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
