# -*- coding: utf-8 -*-
"""US Harness Runner.

manifest.yaml에 정의된 시나리오를 실행하고 결과를 검증한다.

python -m trader.us.harness.runner --scenario all --offline
python -m trader.us.harness.runner --scenario duplicate_order_block --offline
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MANIFEST_PATH = Path(__file__).parent / "manifest.yaml"


def load_manifest() -> list[dict]:
    """manifest.yaml을 로드한다."""
    try:
        import yaml  # type: ignore
        with open(MANIFEST_PATH, "r") as f:
            data = yaml.safe_load(f) or {}
        return data.get("scenarios", [])
    except FileNotFoundError:
        logger.error("[US_HARNESS][ERROR] manifest not found: %s", MANIFEST_PATH)
        return []
    except Exception as exc:
        logger.error("[US_HARNESS][ERROR] manifest load failed: %s", exc)
        return []


def run_scenario(scenario: dict, offline: bool = True) -> dict:
    """단일 시나리오를 실행하고 결과를 반환.

    Returns:
        {"name": ..., "passed": bool, "exit_code": int, "log": str}
    """
    name = scenario.get("name", "unknown")
    command = scenario.get("command", "")
    env_extra = scenario.get("env", {})
    expected = scenario.get("expected_markers", [])
    forbidden = scenario.get("forbidden_markers", [])

    logger.info("[US_HARNESS][SCENARIO] name=%r command=%r", name, command)

    # 환경변수 설정
    env = os.environ.copy()
    for k, v in env_extra.items():
        env[k] = str(v)
    if offline:
        env["DRY_RUN"] = "1"

    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )
        log_text = result.stdout + result.stderr
        exit_code = result.returncode
    except subprocess.TimeoutExpired:
        log_text = "TIMEOUT"
        exit_code = 1

    # Validation
    from trader.us.harness.validators import validate_scenario
    validation = validate_scenario(
        scenario_name=name,
        log_text=log_text,
        expected_markers=expected,
        forbidden_markers=forbidden,
        exit_code=exit_code,
    )

    return {
        "name": name,
        "passed": validation.passed,
        "exit_code": exit_code,
        "log": log_text[:2000],  # 로그 길이 제한
        "missing_markers": validation.missing_markers,
        "forbidden_found": validation.forbidden_found,
    }


def run_all_scenarios(
    scenario_names: list[str] | None = None,
    offline: bool = True,
) -> dict:
    """전체 또는 선택 시나리오 실행.

    Returns:
        {"passed": bool, "results": [...], "summary": {...}}
    """
    logger.info("[US_HARNESS][START] offline=%s", offline)
    scenarios = load_manifest()

    if scenario_names:
        scenarios = [s for s in scenarios if s.get("name") in scenario_names]

    if not scenarios:
        logger.warning("[US_HARNESS][WARN] no scenarios to run")
        return {"passed": True, "results": [], "summary": {"total": 0, "passed": 0, "failed": 0}}

    results = []
    for scenario in scenarios:
        result = run_scenario(scenario, offline=offline)
        results.append(result)
        status = "[US_HARNESS][PASS]" if result["passed"] else "[US_HARNESS][FAIL]"
        logger.info("%s scenario=%r exit_code=%d", status, result["name"], result["exit_code"])

    passed_count = sum(1 for r in results if r["passed"])
    failed_count = len(results) - passed_count
    all_passed = failed_count == 0

    summary = {
        "total": len(results),
        "passed": passed_count,
        "failed": failed_count,
    }

    if all_passed:
        logger.info("[US_HARNESS][ALL_PASS] total=%d", len(results))
    else:
        failed_names = [r["name"] for r in results if not r["passed"]]
        logger.error("[US_HARNESS][FAIL] failed_scenarios=%s", failed_names)

    return {"passed": all_passed, "results": results, "summary": summary}


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="US Harness Runner")
    parser.add_argument(
        "--scenario",
        default="all",
        help="시나리오 이름 (쉼표 구분) 또는 'all'",
    )
    parser.add_argument("--offline", action="store_true", default=True)
    args = parser.parse_args()

    scenario_names = None if args.scenario == "all" else args.scenario.split(",")
    result = run_all_scenarios(scenario_names=scenario_names, offline=args.offline)

    passed = result["summary"]["passed"]
    total = result["summary"]["total"]
    print(f"\n=== US Harness Summary: {passed}/{total} passed ===")
    for r in result["results"]:
        status = "✓" if r["passed"] else "✗"
        print(f"  {status} {r['name']}")
        if not r["passed"]:
            if r["missing_markers"]:
                print(f"    missing: {r['missing_markers']}")
            if r["forbidden_found"]:
                print(f"    forbidden found: {r['forbidden_found']}")

    sys.exit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
