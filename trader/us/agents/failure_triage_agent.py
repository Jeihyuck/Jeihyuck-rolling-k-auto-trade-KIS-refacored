# -*- coding: utf-8 -*-
"""Failure Triage Agent.

GitHub Actions 로그 또는 임의 로그 텍스트를 분류하고 taxonomy 반환.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# classifier type → patch_planner label 변환 맵
_TYPE_TO_LABEL: dict[str, str] = {
    "US_RATE_LIMIT": "RATE_LIMIT",
    "US_KIS_AUTH_ERROR": "AUTH_FAIL",
    "US_KIS_TR_ID_ERROR": "RATE_LIMIT",
    "US_MARKET_GATE_BROKEN": "MARKET_CLOSED",
    "US_MARKET_CLOSED": "MARKET_CLOSED",
    "US_DUPLICATE_ORDER_BLOCKED": "DUPLICATE_ORDER",
    "US_BALANCE_PARSE_ERROR": "UNKNOWN",
    "US_SYMBOL_MAPPING_ERROR": "SYMBOL_REJECT",
    "US_KIS_ENDPOINT_ERROR": "TIMEOUT",
    "US_ORDER_REJECTED": "RISK_GATE_BLOCK",
    "US_DB_CONTRACT_ERROR": "UNKNOWN",
    "US_LEDGER_WRITE_ERROR": "UNKNOWN",
    "US_RECONCILE_MISMATCH": "PARTIAL_FILL",
    "US_HARNESS_ASSERT_FAIL": "HARNESS_SCENARIO_FAIL",
    "US_UNKNOWN_RUNTIME_ERROR": "UNKNOWN",
    "US_UNKNOWN": "UNKNOWN",
}


class FailureTriageAgent:
    """장애 분류 Agent."""

    name = "failure_triage_agent"

    def triage(self, log_text: str) -> dict[str, Any]:
        """log_text 분류 결과 반환."""
        from trader.us.harness.failure_classifier import classify_failure

        result = classify_failure(log_text)
        raw_type = result.get("type", "US_UNKNOWN")
        label = _TYPE_TO_LABEL.get(raw_type, "UNKNOWN")
        logger.info("[US_TRIAGE][%s] raw=%s", label, raw_type)
        return {
            "label": label,
            "raw_type": raw_type,
            "source_length": len(log_text),
        }

    def triage_bulk(self, log_texts: list[str]) -> list[dict[str, Any]]:
        """여러 로그 텍스트를 한꺼번에 분류 (UNKNOWN 포함 전체 반환)."""
        return [self.triage(txt) for txt in (log_texts or [])]
