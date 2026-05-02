# -*- coding: utf-8 -*-
"""US Failure Classifier.

로그 텍스트를 분석해서 실패 유형을 자동 분류한다.
"""
from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Failure taxonomy
# ---------------------------------------------------------------------------
FAILURE_TYPES = [
    "US_KIS_AUTH_ERROR",
    "US_KIS_ENDPOINT_ERROR",
    "US_KIS_TR_ID_ERROR",
    "US_MARKET_CLOSED",
    "US_MARKET_GATE_BROKEN",
    "US_SYMBOL_MAPPING_ERROR",
    "US_BALANCE_PARSE_ERROR",
    "US_ORDER_REJECTED",
    "US_DUPLICATE_ORDER_BLOCKED",
    "US_DB_CONTRACT_ERROR",
    "US_LEDGER_WRITE_ERROR",
    "US_RECONCILE_MISMATCH",
    "US_HARNESS_ASSERT_FAIL",
    "US_UNKNOWN_RUNTIME_ERROR",
    "US_UNKNOWN",
]


def classify_failure(log_text: str) -> dict:
    """로그 텍스트를 분석하여 실패 유형을 반환.

    Args:
        log_text: 로그 전체 텍스트

    Returns:
        {"type": str, "evidence": str}
    """
    text = log_text or ""

    # --- Rate limit errors ---
    if re.search(r"(rate.?limit|EGW00201|too.?many.?req|429)", text, re.IGNORECASE):
        return {"type": "US_RATE_LIMIT", "evidence": _extract_context(text, "rate.?limit|EGW00201|429")}

    # --- Auth errors ---
    if re.search(r"(401|403|unauthorized|invalid.*token|token.*expired)", text, re.IGNORECASE):
        return {"type": "US_KIS_AUTH_ERROR", "evidence": _extract_context(text, "401|403|unauthorized")}

    # --- TR_ID errors ---
    if re.search(r"(TR_ID|tr_id)", text):
        return {"type": "US_KIS_TR_ID_ERROR", "evidence": _extract_context(text, "TR_ID|tr_id")}

    # --- Market gate broken: closed but order went through ---
    if "[US_MARKET][CLOSED]" in text and "[US_ORDER][ACK]" in text:
        return {"type": "US_MARKET_GATE_BROKEN", "evidence": "market closed but order acknowledged"}

    # --- Duplicate order blocked ---
    if "[US_DUPLICATE][BLOCK]" in text:
        return {"type": "US_DUPLICATE_ORDER_BLOCKED", "evidence": _extract_context(text, r"\[US_DUPLICATE\]\[BLOCK\]")}

    # --- Balance parse error ---
    if re.search(r"(balance.*parse|parse.*balance|잔고.*parse|output_9)", text, re.IGNORECASE):
        return {"type": "US_BALANCE_PARSE_ERROR", "evidence": _extract_context(text, "balance.*parse|parse.*balance")}

    # --- Symbol mapping error ---
    if re.search(r"(resolve_exchange|reject_unknown_symbol|unknown.*symbol|symbol.*not.*registry)", text, re.IGNORECASE):
        return {"type": "US_SYMBOL_MAPPING_ERROR", "evidence": _extract_context(text, "unknown.*symbol|symbol.*not")}

    # --- Market closed (non-gate-broken) ---
    if "[US_MARKET][CLOSED]" in text:
        return {"type": "US_MARKET_CLOSED", "evidence": "[US_MARKET][CLOSED] found in log"}

    # --- KIS endpoint errors ---
    if re.search(r"(5\d\d|ConnectionError|Timeout|requests\.exceptions)", text, re.IGNORECASE):
        return {"type": "US_KIS_ENDPOINT_ERROR", "evidence": _extract_context(text, "5\\d\\d|ConnectionError|Timeout")}

    # --- Order rejected ---
    if "[US_ORDER][REJECT]" in text:
        return {"type": "US_ORDER_REJECTED", "evidence": _extract_context(text, r"\[US_ORDER\]\[REJECT\]")}

    # --- DB contract errors ---
    if re.search(r"(IntegrityError|ForeignKeyViolation|UniqueViolation|DBAPIError)", text):
        return {"type": "US_DB_CONTRACT_ERROR", "evidence": _extract_context(text, "IntegrityError|DBAPIError")}

    # --- Ledger write errors ---
    if re.search(r"(ledger.*write|append.*ledger|jsonl.*error)", text, re.IGNORECASE):
        return {"type": "US_LEDGER_WRITE_ERROR", "evidence": _extract_context(text, "ledger.*write|append.*ledger")}

    # --- Reconcile mismatch ---
    if re.search(r"(reconcile.*mismatch|mismatch.*reconcile|US_RECONCILE)", text, re.IGNORECASE):
        return {"type": "US_RECONCILE_MISMATCH", "evidence": _extract_context(text, "reconcile.*mismatch")}

    # --- Harness assertion failure ---
    if "[US_HARNESS][FAIL]" in text:
        return {"type": "US_HARNESS_ASSERT_FAIL", "evidence": _extract_context(text, r"\[US_HARNESS\]\[FAIL\]")}

    # --- Runtime error (traceback) ---
    if "Traceback" in text:
        return {"type": "US_UNKNOWN_RUNTIME_ERROR", "evidence": _extract_traceback(text)}

    return {"type": "US_UNKNOWN", "evidence": ""}


def _extract_context(text: str, pattern: str, window: int = 200) -> str:
    """패턴 주변 텍스트를 추출."""
    try:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            start = max(0, m.start() - window // 2)
            end = min(len(text), m.end() + window // 2)
            return text[start:end].strip()
    except Exception:
        pass
    return ""


def _extract_traceback(text: str) -> str:
    """Traceback 부분 추출."""
    idx = text.find("Traceback")
    if idx >= 0:
        return text[idx:idx + 600].strip()
    return ""


def classify_failures_bulk(log_lines: list[str]) -> list[dict]:
    """여러 로그 라인을 순서대로 분류."""
    results = []
    for line in (log_lines or []):
        result = classify_failure(line)
        if result["type"] != "US_UNKNOWN":
            results.append(result)
    return results
