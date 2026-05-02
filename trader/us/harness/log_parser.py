# -*- coding: utf-8 -*-
"""US Harness Log Parser.

로그 텍스트에서 마커를 추출한다.
"""
from __future__ import annotations

import re


def extract_markers(log_text: str) -> list[str]:
    """로그에서 [US_XXX][YYY] 형식 마커를 추출."""
    if not log_text:
        return []
    return re.findall(r"\[US_[A-Z0-9_]+\]\[[A-Z0-9_]+\]", log_text)


def has_marker(log_text: str, marker: str) -> bool:
    """특정 마커가 로그에 존재하는지 확인."""
    return marker in (log_text or "")


def all_expected_markers_present(log_text: str, expected: list[str]) -> tuple[bool, list[str]]:
    """모든 expected 마커가 존재하는지 확인.

    Returns:
        (all_present: bool, missing: list[str])
    """
    missing = [m for m in expected if not has_marker(log_text, m)]
    return len(missing) == 0, missing


def any_forbidden_marker_present(log_text: str, forbidden: list[str]) -> tuple[bool, list[str]]:
    """금지 마커 중 하나라도 존재하는지 확인.

    Returns:
        (any_present: bool, found: list[str])
    """
    found = [m for m in forbidden if has_marker(log_text, m)]
    return len(found) > 0, found
