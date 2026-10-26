"""PR257 KR native HTTP SLO report from observed (not fabricated) loop logs.

The report requires >=20 observed protected SELL boundary timestamps and
>=20 *all attempted* tick duration samples for p95. Missing/error/timeout
ticks are included in the tick histogram, not silently dropped.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import re

TICK = re.compile(
    r"\[KR_LATENCY\]\[TICK_END\].*?index=(\d+).*?"
    r"total_elapsed_s=([\d.]+).*?result_status=([^\s]+)"
)
SELL = re.compile(
    r"\[KR_LATENCY\]\[NATIVE_HTTP_START\].*?side=SELL.*?"
    r"tick_elapsed_s=([\d.]+).*?tick_index=(\d+)"
)
START = re.compile(r"\[PB1\]\[TICK\]\[CALL_RUN_ONCE\]")
SHA = re.compile(r"\[KR_LATENCY\]\[TICK_END\].*?sha=([^\s]+)")


def p95(values: list[float]) -> float | None:
    clean = [float(v) for v in values if math.isfinite(float(v)) and float(v) >= 0]
    return sorted(clean)[math.ceil(0.95 * len(clean)) - 1] if clean else None


def analyze_kr_latency_lines(lines: list[str], *, min_samples: int = 20) -> dict:
    tick_elapsed: dict[str, float] = {}
    status_by_tick: dict[str, str] = {}
    sell_to_http: dict[str, float] = {}
    observed_sha: set[str] = set()
    for line in lines:
        m = TICK.search(line)
        if m:
            tick_elapsed[m.group(1)] = float(m.group(2))
            status_by_tick[m.group(1)] = m.group(3)
            sha = SHA.search(line)
            if sha:
                observed_sha.add(sha.group(1))
        m = SELL.search(line)
        if m:
            index, seconds = m.group(2), float(m.group(1))
            sell_to_http[index] = min(sell_to_http.get(index, seconds), seconds)
    tick_values = list(tick_elapsed.values())
    sell_values = list(sell_to_http.values())
    tick_p95 = p95(tick_values)
    sell_p95 = p95(sell_values)
    return {
        "sources": sorted(observed_sha),
        "tick_attempts_including_timeout": len(tick_values),
        "tick_failure_or_timeout": sum(
            status not in {"OK", "OK_NO_TRADE", "OK_TRADE"}
            for status in status_by_tick.values()
        ),
        "protected_sell_native_http_samples": len(sell_values),
        "protected_sell_native_http_p95_seconds": sell_p95,
        "all_tick_p95_seconds": tick_p95,
        "protected_sell_target_60s": (
            "PASS" if len(sell_values) >= min_samples and sell_p95 <= 60
            else "FAIL" if len(sell_values) >= min_samples else "INSUFFICIENT_SAMPLES"
        ),
        "all_tick_target_180s": (
            "PASS" if len(tick_values) >= min_samples and tick_p95 <= 180
            else "FAIL" if len(tick_values) >= min_samples else "INSUFFICIENT_SAMPLES"
        ),
        "not_measured": "KIS response/fill completion is separate from native HTTP start",
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Strict KR protected SELL latency SLO")
    p.add_argument("log_file", type=Path)
    p.add_argument("--minimum", type=int, default=20)
    args = p.parse_args(argv)
    lines = args.log_file.read_text(encoding="utf-8", errors="replace").splitlines()
    report = analyze_kr_latency_lines(lines, min_samples=max(20, args.minimum))
    print(json.dumps(report, indent=2))
    return 0 if (
        report["protected_sell_target_60s"] == "PASS"
        and report["all_tick_target_180s"] == "PASS"
    ) else 2


if __name__ == "__main__":
    raise SystemExit(main())
