# -*- coding: utf-8 -*-
"""US Exit Position Resolver.

미국장 전체 보유 종목의 exit input을 표준화한다.

exit 평가에 들어가는 모든 position은 반드시 entry_price를 가져야 한다.
entry_price를 만들 수 없으면 pnl_input_ok=False, entry_price_source="missing"으로 표시한다.

절대 금지:
- 한국장 DB repo import 금지 (us.db.repos 만 사용)
- us_* 이외 테이블 접근 금지
- 특정 종목 심볼 하드코딩 금지
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Any, default: float | None = None) -> float | None:
    """안전한 float 변환. 변환 불가 시 default 반환."""
    if value is None:
        return default
    try:
        f = float(value)
        if f != f:  # NaN
            return default
        return f
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    """안전한 int 변환."""
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _normalize_symbol(symbol: Any) -> str:
    """심볼 대문자 정규화."""
    return str(symbol or "").strip().upper()


# ---------------------------------------------------------------------------
# Entry price resolution helpers
# ---------------------------------------------------------------------------

def _resolve_entry_price_from_position(pos: dict) -> tuple[float | None, str]:
    """position dict에서 직접 entry_price 추출.

    우선순위:
    1. entry_price
    2. avg_price_usd
    3. avg_cost
    4. average_price
    5. avg_buy_price

    Returns:
        (price, source_name) or (None, "")
    """
    for field, source in [
        ("entry_price", "entry_price_field"),
        ("avg_price_usd", "kis_avg_price_usd"),
        ("avg_cost", "avg_cost"),
        ("average_price", "average_price"),
        ("avg_buy_price", "avg_buy_price"),
    ]:
        v = _safe_float(pos.get(field))
        if v is not None and v > 0:
            return v, source
    return None, ""


def _resolve_entry_price_from_buy_amount(pos: dict) -> tuple[float | None, str]:
    """buy_amount_usd / qty 계산.

    Returns:
        (price, "kis_buy_amount_usd") or (None, "")
    """
    qty = _safe_int(pos.get("qty"))
    buy_amount = _safe_float(pos.get("buy_amount_usd"))
    if qty > 0 and buy_amount is not None and buy_amount > 0:
        return round(buy_amount / qty, 6), "kis_buy_amount_usd"
    return None, ""


def _resolve_entry_price_from_pnl_rate(pos: dict) -> tuple[float | None, str]:
    """pnl_rate와 current_price로 entry_price 역산.

    공식: entry_price = current_price / (1 + rate)
    pnl_rate가 절대값 1 이하면 소수점 비율, 초과면 퍼센트로 해석.

    Returns:
        (price, "kis_pnl_rate_fallback") or (None, "")
    """
    # current_price 후보
    current_price: float | None = None
    for field in ("current_price_usd", "current_price", "current_px"):
        v = _safe_float(pos.get(field))
        if v is not None and v > 0:
            current_price = v
            break

    if current_price is None or current_price <= 0:
        return None, ""

    # pnl_rate 후보
    pnl_rate: float | None = None
    for field in ("pnl_rate", "unrealized_pnl_pct", "evlu_pfls_rt"):
        v = _safe_float(pos.get(field))
        if v is not None and v != 0.0:
            pnl_rate = v
            break

    if pnl_rate is None:
        return None, ""

    # 단위 정규화: |rate| > 1 이면 퍼센트
    rate = pnl_rate / 100.0 if abs(pnl_rate) > 1 else pnl_rate

    # -100% 이하이면 무의미 (포지션이 이미 0)
    if rate <= -0.99:
        return None, ""

    entry_price = current_price / (1.0 + rate)
    if entry_price <= 0:
        return None, ""

    return round(entry_price, 6), "kis_pnl_rate_fallback"


# ---------------------------------------------------------------------------
# DB-backed fallbacks (us_positions / us_fills)
# ---------------------------------------------------------------------------

def _resolve_from_us_positions_db(
    symbol: str,
    as_of: str | None,
) -> tuple[float | None, str]:
    """us_positions DB에서 avg_cost 조회."""
    try:
        from trader.us.db.repos import load_us_positions_by_symbols
        rows = load_us_positions_by_symbols([symbol], as_of=as_of)
        row = rows.get(symbol)
        if row:
            v = _safe_float(row.get("avg_cost"))
            if v is not None and v > 0:
                return v, "us_positions_avg_cost"
    except Exception as exc:
        logger.debug("[US_EXIT_RESOLVER][DB_POS_FAIL] symbol=%s err=%s", symbol, exc)
    return None, ""


def _resolve_from_us_fills_db(
    symbol: str,
    trade_date: str | None,
) -> tuple[float | None, str]:
    """us_fills DB에서 최신 BUY price_usd 조회."""
    try:
        from trader.us.db.repos import load_latest_us_buy_fills_by_symbols
        rows = load_latest_us_buy_fills_by_symbols([symbol], trade_date=trade_date)
        row = rows.get(symbol)
        if row:
            v = _safe_float(row.get("price_usd"))
            if v is not None and v > 0:
                return v, "us_fills_latest_buy"
    except Exception as exc:
        logger.debug("[US_EXIT_RESOLVER][DB_FILL_FAIL] symbol=%s err=%s", symbol, exc)
    return None, ""


# ---------------------------------------------------------------------------
# Single position enrichment
# ---------------------------------------------------------------------------

def _enrich_single_position(
    pos: dict,
    *,
    trade_date: str | None,
) -> dict:
    """단일 position의 entry_price를 표준 contract로 채운다.

    이미 유효한 entry_price가 있으면 그대로 반환.
    없으면 순서대로 fallback을 시도한다.
    """
    symbol = _normalize_symbol(pos.get("symbol", ""))
    exchange = str(pos.get("exchange") or "NASDAQ").strip() or "NASDAQ"
    qty = _safe_int(pos.get("qty"))

    # qty <= 0 은 exit 대상 아님
    if qty <= 0:
        return {
            **pos,
            "symbol": symbol,
            "exchange": exchange,
            "qty": qty,
            "pnl_input_ok": False,
            "entry_price_source": "qty_zero",
        }

    # 이미 entry_price 있으면 source만 보정
    existing_ep = _safe_float(pos.get("entry_price"))
    existing_src = pos.get("entry_price_source") or ""
    if existing_ep is not None and existing_ep > 0 and existing_src:
        return {
            **pos,
            "symbol": symbol,
            "exchange": exchange,
            "qty": qty,
            "entry_price": existing_ep,
            "pnl_input_ok": True,
        }

    # --- Resolution chain ---
    ep: float | None = None
    src: str = ""

    # 1-5: position 필드 직접
    ep, src = _resolve_entry_price_from_position(pos)

    # 6: buy_amount_usd / qty
    if ep is None:
        ep, src = _resolve_entry_price_from_buy_amount(pos)

    # 7: pnl_rate 역산
    if ep is None:
        ep, src = _resolve_entry_price_from_pnl_rate(pos)

    # 8: us_positions DB
    if ep is None:
        ep, src = _resolve_from_us_positions_db(symbol, as_of=trade_date)

    # 9: us_fills DB
    if ep is None:
        ep, src = _resolve_from_us_fills_db(symbol, trade_date=trade_date)

    enriched = {
        **pos,
        "symbol": symbol,
        "exchange": exchange,
        "qty": qty,
    }

    if ep is not None and ep > 0:
        enriched["entry_price"] = ep
        enriched["entry_price_source"] = src
        enriched["pnl_input_ok"] = True
    else:
        enriched["entry_price"] = 0.0
        enriched["entry_price_source"] = "missing"
        enriched["pnl_input_ok"] = False
        logger.warning(
            "[US_EXIT_RESOLVER][PNL_MISSING] symbol=%s qty=%s "
            "entry_price=%s avg_price_usd=%s avg_cost=%s buy_amount_usd=%s pnl_rate=%s",
            symbol,
            qty,
            pos.get("entry_price"),
            pos.get("avg_price_usd"),
            pos.get("avg_cost"),
            pos.get("buy_amount_usd"),
            pos.get("pnl_rate"),
        )

    # max_price fallback: entry_price 기준
    if not pos.get("max_price") and not pos.get("high_watermark"):
        current_price_v = _safe_float(
            pos.get("current_price_usd") or pos.get("current_price") or pos.get("current_px")
        )
        base = enriched.get("entry_price") or 0.0
        if current_price_v and current_price_v > 0:
            enriched["max_price"] = max(base, current_price_v)
        elif base > 0:
            enriched["max_price"] = base

    return enriched


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def enrich_us_positions_for_exit(
    positions: list[dict],
    *,
    trade_date: str,
    env: str = "practice",
    provider: Any = None,
) -> tuple[list[dict], dict]:
    """미국장 전체 보유 종목의 exit input을 표준화한다.

    Parameters
    ----------
    positions : list[dict]
        KIS reconcile 또는 DB에서 온 raw position 목록
    trade_date : str
        거래일 (YYYY-MM-DD)
    env : str
        실행 환경 (practice / real / live)
    provider : optional
        USDataProvider (현재 미사용 — 향후 real-time price 보강용)

    Returns
    -------
    (enriched_positions, meta)
        enriched_positions: entry_price가 채워진 position 목록 (qty <= 0 제외)
        meta: resolution 통계 dict
    """
    if not positions:
        return [], {
            "total": 0, "ok": 0, "missing": 0,
            "sources": {}, "missing_symbols": [],
        }

    enriched_list: list[dict] = []
    source_counts: dict[str, int] = {}
    missing_symbols: list[str] = []
    ok_count = 0

    for pos in positions:
        ep = _enrich_single_position(pos, trade_date=trade_date)
        qty = ep.get("qty", 0)

        # qty <= 0 제외
        if qty <= 0:
            continue

        enriched_list.append(ep)

        src = ep.get("entry_price_source", "missing")
        source_counts[src] = source_counts.get(src, 0) + 1

        if ep.get("pnl_input_ok"):
            ok_count += 1
        else:
            missing_symbols.append(ep.get("symbol", "?"))

    total = len(enriched_list)
    missing_count = total - ok_count

    meta = {
        "total": total,
        "ok": ok_count,
        "missing": missing_count,
        "sources": source_counts,
        "missing_symbols": missing_symbols,
    }

    logger.info(
        "[US_EXIT_RESOLVER][DONE] total=%d ok=%d missing=%d sources=%s missing_symbols=%s",
        total,
        ok_count,
        missing_count,
        source_counts,
        missing_symbols,
    )

    return enriched_list, meta
