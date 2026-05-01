# -*- coding: utf-8 -*-
"""Patch Planner Agent.

failure_triage 결과를 받아 patch plan dict를 생성.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

logger = logging.getLogger(__name__)

# --- failure type → 자동 수정 계획 매핑 ---
_FAILURE_PLANS: dict[str, dict[str, Any]] = {
    "RISK_GATE_BLOCK": {
        "files_to_change": ["trader/us/execution/risk_gate.py"],
        "commands": ["pytest -q tests/us/test_risk_gate.py"],
        "safe_to_apply": False,
    },
    "RATE_LIMIT": {
        "files_to_change": ["trader/us/execution/kis_us_client.py"],
        "commands": ["pytest -q tests/us/test_kis_us_client.py"],
        "safe_to_apply": True,
    },
    "AUTH_FAIL": {
        "files_to_change": ["trader/us/execution/kis_us_client.py"],
        "commands": ["pytest -q tests/us/test_kis_us_client.py"],
        "safe_to_apply": False,
    },
    "DUPLICATE_ORDER": {
        "files_to_change": ["trader/us/execution/order_router.py"],
        "commands": ["pytest -q tests/us/test_order_router.py"],
        "safe_to_apply": True,
    },
    "INSUFFICIENT_CASH": {
        "files_to_change": ["trader/us/execution/risk_gate.py"],
        "commands": ["pytest -q tests/us/test_risk_gate.py"],
        "safe_to_apply": True,
    },
    "MARKET_CLOSED": {
        "files_to_change": ["trader/us/market_calendar.py"],
        "commands": ["pytest -q tests/us/test_market_calendar.py"],
        "safe_to_apply": True,
    },
    "PARTIAL_FILL": {
        "files_to_change": ["trader/us/execution/reconcile.py"],
        "commands": ["pytest -q tests/us/test_reconcile.py"],
        "safe_to_apply": True,
    },
    "SYMBOL_REJECT": {
        "files_to_change": ["trader/us/symbols.py"],
        "commands": ["pytest -q tests/us/test_symbols.py"],
        "safe_to_apply": True,
    },
    "IMPORT_ERROR": {
        "files_to_change": [],
        "commands": ["pip install -r requirements.txt", "pytest -q tests/us"],
        "safe_to_apply": False,
    },
    "TIMEOUT": {
        "files_to_change": ["trader/us/execution/kis_us_client.py"],
        "commands": ["pytest -q tests/us/test_kis_us_client.py"],
        "safe_to_apply": True,
    },
    "ENV_MISSING": {
        "files_to_change": ["trader/us/config.py"],
        "commands": ["pytest -q tests/us/test_config.py"],
        "safe_to_apply": False,
    },
    "HARNESS_SCENARIO_FAIL": {
        "files_to_change": ["trader/us/harness/"],
        "commands": ["python -m trader.us.harness.runner --scenario all --offline"],
        "safe_to_apply": True,
    },
    "UNKNOWN": {
        "files_to_change": [],
        "commands": ["pytest -q tests/us"],
        "safe_to_apply": False,
    },
}


class PatchPlannerAgent:
    """Patch plan 생성 Agent."""

    name = "patch_planner_agent"

    def plan(self, triage_result: dict[str, Any]) -> dict[str, Any]:
        """triage 결과 → patch plan dict."""
        failure_type: str = triage_result.get("label", "UNKNOWN")
        template = _FAILURE_PLANS.get(failure_type, _FAILURE_PLANS["UNKNOWN"])
        patch_id = str(uuid.uuid4())[:8]
        plan: dict[str, Any] = {
            "patch_id": patch_id,
            "failure_type": failure_type,
            "files_to_change": list(template["files_to_change"]),
            "commands": list(template["commands"]),
            "safe_to_apply": template["safe_to_apply"],
        }
        logger.info("[US_PATCH_PLANNER][%s] patch_id=%s safe=%s",
                    failure_type, patch_id, plan["safe_to_apply"])
        return plan

    def plan_bulk(self, triage_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """여러 triage 결과에 대한 plan 목록 반환."""
        return [self.plan(r) for r in triage_results]
