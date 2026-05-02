# -*- coding: utf-8 -*-
"""KIS 해외주식 주문 응답 파서.

KIS 응답 필드명은 환경(vts/prod)에 따라 다를 수 있으므로
여러 후보 필드를 순서대로 시도한다.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# KIS 해외주식 주문 응답의 order_no 후보 필드 (우선순위 순)
_ORDER_NO_CANDIDATES = [
    ("output", "ODNO"),
    ("output", "odno"),
    ("output", "KRX_FWDG_ORD_ORGNO"),
    ("output", "ORD_NO"),
    ("output", "order_no"),
]


def extract_order_no(resp: dict) -> str:
    """KIS 주문 응답에서 order_no를 추출한다.

    Args:
        resp: KIS API 응답 dict

    Returns:
        order_no 문자열. 못 찾으면 빈 문자열.
    """
    if not isinstance(resp, dict):
        return ""

    for parent_key, field in _ORDER_NO_CANDIDATES:
        parent = resp.get(parent_key, {})
        if isinstance(parent, dict):
            val = parent.get(field)
            if val:
                return str(val).strip()

    # 최상위 레벨에서도 시도
    for _, field in _ORDER_NO_CANDIDATES:
        val = resp.get(field)
        if val:
            return str(val).strip()

    logger.debug("[KIS_PARSER][WARN] order_no not found in response keys=%s",
                 list(resp.get("output", {}).keys()) if resp.get("output") else list(resp.keys()))
    return ""


def extract_rt_cd(resp: dict) -> str:
    """응답 성공 코드 추출. '0'이면 성공."""
    return str(resp.get("rt_cd", resp.get("RT_CD", ""))).strip()


def is_success_response(resp: dict) -> bool:
    """KIS 응답이 성공인지 확인."""
    return extract_rt_cd(resp) == "0"
