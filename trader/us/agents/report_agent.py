# -*- coding: utf-8 -*-
"""Report Agent.

일일 요약 생성 (orders / fills / errors / next actions).
"""
from __future__ import annotations

import datetime
import logging
from typing import Any

logger = logging.getLogger(__name__)


class ReportAgent:
    """일일 리포트 Agent."""

    name = "report_agent"

    def generate(
        self,
        intents: list[dict] | None = None,
        execution_results: list[dict] | None = None,
        risk_results: list[dict] | None = None,
        harness_result: dict | None = None,
        extra: dict | None = None,
    ) -> dict[str, Any]:
        """일일 요약 dict 반환."""
        intents = intents or []
        execution_results = execution_results or []
        risk_results = risk_results or []

        total_intents = len(intents)
        risk_pass = sum(1 for r in risk_results if r.get("status") == "PASS")
        risk_block = sum(1 for r in risk_results if r.get("status") == "BLOCK")
        exec_ack = sum(1 for r in execution_results if r.get("status") in ("DRY_RUN", "ACK"))
        exec_fail = sum(1 for r in execution_results if r.get("status") not in ("DRY_RUN", "ACK"))

        harness_ok = harness_result.get("overall", "FAIL") == "PASS" if harness_result else None

        next_actions: list[str] = []
        if risk_block > 0:
            next_actions.append(f"[RISK] {risk_block}건 block 원인 확인 필요")
        if exec_fail > 0:
            next_actions.append(f"[EXEC] {exec_fail}건 실행 실패 확인 필요")
        if harness_ok is False:
            next_actions.append("[HARNESS] 시나리오 실패 — failure_triage_agent 실행 권장")
        if not next_actions:
            next_actions.append("이상 없음 — 다음 거래일 자동 실행 대기")

        report: dict[str, Any] = {
            "report_date": datetime.date.today().isoformat(),
            "total_intents": total_intents,
            "risk_pass": risk_pass,
            "risk_block": risk_block,
            "exec_ack": exec_ack,
            "exec_fail": exec_fail,
            "harness_ok": harness_ok,
            "next_actions": next_actions,
        }
        if extra:
            report.update(extra)

        logger.info("[US_REPORT_AGENT][DONE] date=%s intents=%d ack=%d",
                    report["report_date"], total_intents, exec_ack)
        return report
