# -*- coding: utf-8 -*-
"""trader/us/utils/pnl_utils.py

미국장 전용 PnL 유틸리티.

한국장 normalize 함수는 수정하지 않는다.
이 모듈은 미국장 코드에서만 import한다.

주요 기능:
- normalize_us_position: KIS balance row → 표준화된 포지션 dict
- safe_float / safe_int: 안전한 타입 변환
- first_present: 후보 키 목록 중 첫 번째 유효 값 추출
- get_table_columns_sync: DB 테이블 컬럼 목록 조회 (동기)
- pick_column_from_list: 후보 컬럼 중 실제 존재하는 컬럼 선택
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 안전한 타입 변환
# ---------------------------------------------------------------------------


def safe_float(value: Any, default: float = 0.0) -> float:
    """안전하게 float로 변환한다."""
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if value == "":
                return default
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    """안전하게 int로 변환한다."""
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if value == "":
                return default
        return int(float(value))
    except Exception:
        return default


def first_present(row: dict, keys: list[str], default: Any = None) -> Any:
    """후보 키 목록 중 첫 번째로 유효한(None/'' 아닌) 값을 반환한다."""
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return default


# ---------------------------------------------------------------------------
# 미국장 포지션 표준화
# ---------------------------------------------------------------------------


def normalize_us_position(row: dict) -> dict:
    """KIS balance row를 미국장 표준 포지션 dict로 변환한다.

    필드 후보 우선순위:
    - symbol: symbol > ticker > ovrs_pdno > pdno > code
    - qty:    qty > quantity > hldg_qty > ovrs_cblc_qty > ord_psbl_qty
    - avg_price: avg_price > average_price > pchs_avg_pric > avg_unpr > buy_avg_price
    - last_price: last_price > current > current_price > market_price > ovrs_now_pric > ovrs_now_pric1

    수익률 계산:
      market_value_usd = qty * last_price
      cost_basis_usd   = qty * avg_price
      unrealized_pnl_usd = market_value_usd - cost_basis_usd
      unrealized_pnl_pct = (unrealized_pnl_usd / cost_basis_usd) * 100   [cost_basis > 0]

    절대 사용 금지:
    - 누적곱 수익률
    - 100% 본전 방식
    - last_price=0일 때 -100%를 정상으로 처리
    """
    symbol = str(
        first_present(row, ["symbol", "ticker", "ovrs_pdno", "pdno", "code"], "") or ""
    ).strip().upper()

    qty = safe_int(
        first_present(row, ["qty", "quantity", "hldg_qty", "ovrs_cblc_qty", "ord_psbl_qty"], 0)
    )

    avg_price = safe_float(
        first_present(
            row,
            ["avg_price", "average_price", "pchs_avg_pric", "avg_unpr", "buy_avg_price"],
            0,
        )
    )

    last_price = safe_float(
        first_present(
            row,
            [
                "last_price",
                "current",
                "current_price",
                "market_price",
                "ovrs_now_pric",
                "ovrs_now_pric1",
            ],
            0,
        )
    )

    price_missing = bool(qty > 0 and last_price <= 0)

    # ── 경고: 수량이 있는데 가격이 없으면 로그 기록 ──
    if price_missing:
        logger.warning(
            "[US_PNL][PRICE_MISSING] symbol=%s qty=%s row_keys=%s",
            symbol,
            qty,
            list(row.keys()),
        )

    cost_basis = qty * avg_price
    market_value = qty * last_price
    pnl = market_value - cost_basis
    pnl_pct = (pnl / cost_basis * 100.0) if cost_basis > 0 else 0.0

    # ── 경고: 수량이 있는데 평가금액이 0이면 로그 기록 ──
    if qty > 0 and market_value <= 0:
        logger.warning(
            "[US_PNL][MARKET_VALUE_ZERO] symbol=%s qty=%s last_price=%s",
            symbol,
            qty,
            last_price,
        )

    return {
        "symbol": symbol,
        "qty": qty,
        "avg_price": avg_price,
        "last_price": last_price,
        "cost_basis_usd": cost_basis,
        "market_value_usd": market_value,
        "unrealized_pnl_usd": pnl,
        "unrealized_pnl_pct": pnl_pct,
        "price_missing": price_missing,
        "currency": "USD",
    }


# ---------------------------------------------------------------------------
# DB 스키마 유틸
# ---------------------------------------------------------------------------


def get_table_columns_sync(engine: Any, table_name: str) -> set[str]:
    """SQLAlchemy engine을 사용해 테이블 컬럼 목록을 조회한다 (동기).

    테이블이 없거나 조회 실패 시 빈 set 반환.
    """
    try:
        from sqlalchemy import text as sa_text

        with engine.connect() as conn:
            rows = conn.execute(
                sa_text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = :tbl
                    """
                ),
                {"tbl": table_name},
            ).fetchall()
        return {r[0] for r in rows}
    except Exception as exc:
        logger.warning(
            "[US_SCHEMA][COLUMNS_FETCH_FAILED] table=%s error=%s",
            table_name,
            exc,
        )
        return set()


def pick_column_from_list(
    available: "set[str] | list[str]",
    candidates: list[str],
) -> "str | None":
    """후보 컬럼 목록 중 실제 존재하는 첫 번째 컬럼을 반환한다.

    Args:
        available: 실제 DB에 존재하는 컬럼 이름 집합
        candidates: 우선순위 후보 컬럼 이름 목록

    Returns:
        존재하는 첫 번째 컬럼 이름, 없으면 None
    """
    for col in candidates:
        if col in available:
            return col
    return None
