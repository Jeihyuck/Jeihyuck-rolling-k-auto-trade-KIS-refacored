# -*- coding: utf-8 -*-
"""US Harness Validators.

시나리오 결과를 검증하는 validator.
"""
from __future__ import annotations

import logging

from trader.us.harness.log_parser import all_expected_markers_present, any_forbidden_marker_present

logger = logging.getLogger(__name__)


class ValidationResult:
    def __init__(
        self,
        passed: bool,
        scenario_name: str,
        missing_markers: list[str] | None = None,
        forbidden_found: list[str] | None = None,
        message: str = "",
    ) -> None:
        self.passed = passed
        self.scenario_name = scenario_name
        self.missing_markers = missing_markers or []
        self.forbidden_found = forbidden_found or []
        self.message = message

    def __repr__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"ValidationResult({status}, {self.scenario_name!r}, msg={self.message!r})"


def validate_scenario(
    scenario_name: str,
    log_text: str,
    expected_markers: list[str],
    forbidden_markers: list[str],
    exit_code: int = 0,
) -> ValidationResult:
    """시나리오 로그를 검증한다."""
    # exit code 확인
    if exit_code != 0:
        msg = f"[US_HARNESS][ASSERT] scenario={scenario_name!r} exit_code={exit_code} expected=0"
        logger.warning("[US_HARNESS][FAIL] %s", msg)
        return ValidationResult(False, scenario_name, message=msg)

    # expected markers
    all_present, missing = all_expected_markers_present(log_text, expected_markers)
    if not all_present:
        msg = f"missing_markers={missing}"
        logger.warning("[US_HARNESS][FAIL] scenario=%r %s", scenario_name, msg)
        return ValidationResult(False, scenario_name, missing_markers=missing, message=msg)

    # forbidden markers
    any_found, found = any_forbidden_marker_present(log_text, forbidden_markers)
    if any_found:
        msg = f"forbidden_markers_found={found}"
        logger.warning("[US_HARNESS][FAIL] scenario=%r %s", scenario_name, msg)
        return ValidationResult(False, scenario_name, forbidden_found=found, message=msg)

    logger.info("[US_HARNESS][PASS] scenario=%r", scenario_name)
    return ValidationResult(True, scenario_name)
