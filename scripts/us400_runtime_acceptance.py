#!/usr/bin/env python3
"""Read-only US400 same-workload timing check; never a native/order GO."""
import argparse
import json
from pathlib import Path
from trader.us.runner.runtime_acceptance_evidence import summarize_runtime,compare_runtime


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--baseline",type=Path,required=True)
    p.add_argument("--candidate",type=Path,required=True)
    p.add_argument("--baseline-sha",required=True)
    p.add_argument("--candidate-sha",required=True)
    p.add_argument("--output",type=Path)
    a=p.parse_args()
    def lines(path):
        paths=[path] if path.is_file() else sorted(path.rglob("*.log"))
        for file in paths:
            with file.open(encoding="utf-8",errors="replace") as stream:
                yield from stream
    report=compare_runtime(summarize_runtime(lines(a.baseline),expected_sha=a.baseline_sha),
                           summarize_runtime(lines(a.candidate),expected_sha=a.candidate_sha))
    encoded=json.dumps(report,indent=2,sort_keys=True)
    if a.output: a.output.write_text(encoded+"\n",encoding="utf-8")
    print(encoded)
    return 0 if report["status"]=="PASS_LOG_TIMING_ONLY" else 2


if __name__=="__main__":
    raise SystemExit(main())
