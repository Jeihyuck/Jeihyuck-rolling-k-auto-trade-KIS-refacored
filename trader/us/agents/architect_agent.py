# -*- coding: utf-8 -*-
"""Architect Agent.

전체 구조 점검:
- 국내/미국 코드 경계 위반 탐지
- 어떤 agent가 처리할지 route
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_BOUNDARY_VIOLATIONS = [
    ("KRX", "한국 시장 코드"),
    ("KOSPI", "코스피 코드"),
    ("KOSDAQ", "코스닥 코드"),
    ("KST", "한국 시간대"),
    ("pykrx", "국내 OHLCV 라이브러리"),
    (".zfill(6)", "국내 6자리 종목코드 변환"),
    ("zfill", "국내 종목코드 zero-padding"),
]


class ArchitectAgent:
    """전체 구조 점검 Agent."""

    name = "architect_agent"

    def check_boundary_violations(self, file_content: str, file_path: str = "") -> list[dict]:
        """US 코드 파일에서 국내 전용 코드가 있는지 탐지."""
        violations = []
        for keyword, description in _BOUNDARY_VIOLATIONS:
            if keyword in file_content:
                violations.append({
                    "file": file_path,
                    "keyword": keyword,
                    "description": description,
                })
                logger.warning(
                    "[US_ARCHITECT][BOUNDARY_VIOLATION] file=%r keyword=%r desc=%r",
                    file_path, keyword, description,
                )
        return violations

    def route_task(self, task_type: str) -> str:
        """태스크 유형에 따라 담당 agent를 반환."""
        routing = {
            "strategy_scoring": "strategy_agent",
            "order_execution": "execution_agent",
            "risk_check": "risk_agent",
            "test_run": "qa_harness_agent",
            "failure_analysis": "failure_triage_agent",
            "patch_planning": "patch_planner_agent",
            "daily_report": "report_agent",
        }
        return routing.get(task_type, "architect_agent")

    def inspect(self) -> dict:
        """현재 구조 상태 점검."""
        from pathlib import Path
        us_dir = Path(__file__).resolve().parents[1]
        us_files = list(us_dir.rglob("*.py"))

        all_violations = []
        for f in us_files:
            try:
                content = f.read_text()
                violations = self.check_boundary_violations(content, str(f))
                all_violations.extend(violations)
            except Exception:
                pass

        return {
            "us_files_checked": len(us_files),
            "boundary_violations": all_violations,
            "status": "OK" if not all_violations else "WARN",
        }
