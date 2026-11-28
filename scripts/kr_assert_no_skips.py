"""Fail required offline KR CI suites on skips or empty test reports.

This gate does not certify separate native authenticated broker evidence.
"""
from __future__ import annotations

import sys
from pathlib import Path
from xml.etree import ElementTree as ET


def check(path: str) -> None:
    cases = ET.parse(Path(path)).getroot().findall(".//testcase")
    if not cases:
        raise SystemExit("KR CI: no collected testcases in JUnit report")
    skipped = [(case.get("classname"), case.get("name"), node.get("message"))
               for case in cases for node in case.findall("skipped")]
    if skipped:
        for item in skipped:
            print("KR CI unexpected skip:", *item, sep=" | ", file=sys.stderr)
        raise SystemExit(f"KR CI incomplete: {len(skipped)} skipped tests")
    print(f"KR CI complete: {len(cases)} testcases, zero skipped")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: kr_assert_no_skips.py JUNIT_XML")
    check(sys.argv[1])
