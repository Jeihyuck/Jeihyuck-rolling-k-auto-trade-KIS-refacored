# -*- coding: utf-8 -*-
"""QA Harness Agent.

pytest + harness runner를 실행하고 결과를 반환.
"""
from __future__ import annotations

import logging
import subprocess
import sys

logger = logging.getLogger(__name__)


class QAHarnessAgent:
    """테스트 + harness 자동 실행 Agent."""

    name = "qa_harness_agent"

    def run_pytest(self) -> dict:
        """pytest -q tests/us 실행."""
        cmd = [sys.executable, "-m", "pytest", "-q", "tests/us"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        passed = result.returncode == 0
        logger.info("[US_QA_AGENT][PYTEST] returncode=%d", result.returncode)
        return {
            "check": "pytest",
            "passed": passed,
            "returncode": result.returncode,
            "stdout": result.stdout[-3000:],
            "stderr": result.stderr[-1000:],
        }

    def run_harness(self) -> dict:
        """harness runner --scenario all --offline 실행."""
        cmd = [
            sys.executable, "-m", "trader.us.harness.runner",
            "--scenario", "all",
            "--offline",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        passed = result.returncode == 0
        logger.info("[US_QA_AGENT][HARNESS] returncode=%d", result.returncode)
        return {
            "check": "harness",
            "passed": passed,
            "returncode": result.returncode,
            "stdout": result.stdout[-3000:],
            "stderr": result.stderr[-1000:],
        }

    def run_all(self) -> dict:
        """pytest 와 harness 모두 실행."""
        pytest_result = self.run_pytest()
        harness_result = self.run_harness()
        all_pass = pytest_result["passed"] and harness_result["passed"]
        if all_pass:
            logger.info("[US_QA_AGENT][ALL_PASS]")
        else:
            logger.warning("[US_QA_AGENT][FAIL] pytest=%s harness=%s",
                           pytest_result["passed"], harness_result["passed"])
        return {
            "overall": "PASS" if all_pass else "FAIL",
            "pytest": pytest_result,
            "harness": harness_result,
        }
