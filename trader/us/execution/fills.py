# -*- coding: utf-8 -*-
"""US Fills 조회.

당일 체결 내역 조회 및 파싱.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

def _first_nonblank(row: dict, *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _combine_kis_date_time(date_value: str | None, *time_values: str | None) -> str:
    date_text = str(date_value or "").replace("-", "").strip()
    time_text = ""
    for value in time_values:
        if value not in (None, ""):
            time_text = str(value).replace(":", "").strip()
            break
    if date_text and time_text:
        time_text = time_text.zfill(6)[:6]
        return f"{date_text[:4]}-{date_text[4:6]}-{date_text[6:8]}T{time_text[:2]}:{time_text[2:4]}:{time_text[4:6]}"
    return date_text


def get_fills_today(
    provider: Any | None = None,
    signal_only: bool = False,
    trade_date: str | None = None,
) -> dict:
    """당일 체결 내역 반환.

    Args:
        provider: USDataProvider 인스턴스 (None이면 offline 기본 provider 사용)
        signal_only: True이면 KIS API 호출 없이 DB 데이터만 사용
        trade_date: YYYY-MM-DD 형식 거래일 (None이면 NY 기준 오늘)

    Returns:
        {
            "status": "OK"|"TEMP_ERROR"|"CONTRACT_ERROR",
            "fills": [...],
            "error": str|None,
            "error_type": str|None
        }
        - status="OK": 성공 (fills는 체결 목록, 빈 리스트도 OK)
        - status="TEMP_ERROR": 일시적 오류 (재시도 가능)
        - status="CONTRACT_ERROR": 파라미터/계약 오류 (재시도 불가)
    """
    if provider is None:
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)

    # signal_only 모드: KIS 호출 차단, DB 데이터만 사용
    if signal_only or os.getenv("US_SIGNAL_ONLY") == "1":
        logger.info("[US_FILLS][SIGNAL_ONLY] skipping KIS fill query, using DB-only")
        try:
            from trader.us.db.repos import load_today_fills
            db_fills = load_today_fills(trade_date=trade_date)
            logger.info("[US_FILLS][DB_ONLY] count=%d", len(db_fills))
            return {
                "status": "OK",
                "fills": db_fills,
                "error": None,
                "error_type": None,
            }
        except Exception as exc:
            logger.warning("[US_FILLS][DB_ONLY][WARN] %s", exc)
            return {
                "status": "TEMP_ERROR",
                "fills": [],
                "error": str(exc),
                "error_type": "DB_QUERY",
            }

    if getattr(provider, "_offline", False):
        logger.debug("[US_FILLS][OFFLINE] returning stub fills")
        return {"status": "OK", "fills": [], "error": None, "error_type": None}

    try:
        client = provider._get_client()
        raw = client.get_us_fills_today(trade_date=trade_date)
        observed_at = datetime.now(timezone.utc).isoformat()
        fills = []
        for row in raw:
            order_no = _first_nonblank(row, "odno", "order_no", "ODNO")
            order_timestamp = _combine_kis_date_time(
                _first_nonblank(row, "ord_dt", "ORD_DT"),
                _first_nonblank(row, "ord_tmd", "ORD_TMD"),
            )
            requested_qty = int(row.get("ft_ord_qty") or row.get("ord_qty") or 0)
            cumulative_filled_qty = int(row.get("ft_ccld_qty", 0) or 0)
            remaining_qty = int(row.get("nccs_qty") or row.get("rmn_qty") or 0)
            avg_price_usd = float(row.get("ft_ccld_unpr3", 0) or 0)
            if cumulative_filled_qty <= 0:
                logger.debug(
                    "[US_FILLS][SKIP_UNFILLED_ORDER] order_no=%s symbol=%s remaining_qty=%s",
                    order_no,
                    row.get("pdno", ""),
                    remaining_qty,
                )
                continue
            fills.append({
                "symbol": row.get("pdno", ""),
                "exchange": row.get("ovrs_excg_cd", ""),
                "side": "BUY" if row.get("sll_buy_dvsn_cd") == "02" else "SELL",
                "qty": cumulative_filled_qty,
                "price": avg_price_usd,
                "avg_price_usd": avg_price_usd,
                "filled_at": order_timestamp or row.get("ord_dt", ""),
                "observed_at": observed_at,
                "order_timestamp": order_timestamp,
                "order_no": order_no,
                "requested_qty": requested_qty,
                "cumulative_filled_qty": cumulative_filled_qty,
                "remaining_qty": remaining_qty,
                "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                "meta": {
                    "is_synthetic": False,
                    "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                    "source_endpoint": "KIS_INQUIRE_CCNL",
                    "cumulative_filled_qty": cumulative_filled_qty,
                    "remaining_qty": remaining_qty,
                    "requested_qty": requested_qty,
                    "order_timestamp": order_timestamp,
                    "observed_at": observed_at,
                },
                "raw": row,
            })
        logger.info("[US_FILLS][OK] count=%d", len(fills))
        return {"status": "OK", "fills": fills, "error": None, "error_type": None}
    except Exception as exc:
        from trader.us.execution.kis_us_client import (
            KisUSTemporaryError,
            KisUSClientError,
        )
        
        error_msg = str(exc)
        
        # Contract error (non-temporary) 처리
        if isinstance(exc, KisUSClientError) and not isinstance(exc, KisUSTemporaryError):
            logger.error("[US_FILLS][ERROR][CONTRACT] %s", error_msg)
            return {
                "status": "CONTRACT_ERROR",
                "fills": [],
                "error": error_msg,
                "error_type": "ORD_DT_PARAM" if "ORD_DT" in error_msg or "INPUT_FIELD_NAME" in error_msg else "CONTRACT",
            }
        # Temporary error 처리
        elif isinstance(exc, KisUSTemporaryError):
            logger.warning("[US_FILLS][ERROR][TEMP] type=RATE_LIMIT msg=%s", error_msg)
            return {
                "status": "TEMP_ERROR",
                "fills": [],
                "error": error_msg,
                "error_type": "RATE_LIMIT",
            }
        else:
            # 기타 예외
            logger.error("[US_FILLS][ERROR] %s", error_msg)
            return {
                "status": "TEMP_ERROR",
                "fills": [],
                "error": error_msg,
                "error_type": "UNKNOWN",
            }
