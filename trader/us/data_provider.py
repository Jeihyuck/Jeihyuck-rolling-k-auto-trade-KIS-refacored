# -*- coding: utf-8 -*-
"""미국주식 Data Provider.

offline 모드에서는 fixture 데이터를 반환한다.
online 모드에서는 KisUSClient를 통해 실제 데이터를 가져온다.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from trader.us.utils.order_no import normalize_us_order_no

logger = logging.getLogger(__name__)
_DAILY_RAW_KEYS_LOGGED: set[str] = set()


def _get_engine():
    """SQLAlchemy engine 반환."""
    try:
        from trader.db.engine import get_engine as _get_engine_impl
        return _get_engine_impl()
    except ImportError:
        # fallback: no DB available
        return None


def _make_stub_daily(symbol: str, count: int = 60) -> list[dict]:
    """offline/test 용 stub daily price 데이터 (xymd 오름차순)."""
    base_price = 100.0
    prices = []
    try:
        from trader.us.market_calendar import previous_completed_us_session, is_us_trading_day
        d = previous_completed_us_session(date.today() + timedelta(days=1))
        dates = []
        cur = d
        while len(dates) < count:
            if is_us_trading_day(cur):
                dates.append(cur)
            cur -= timedelta(days=1)
        dates = list(reversed(dates))
    except Exception:
        d = date.today()
        dates = [d - timedelta(days=count - i - 1) for i in range(count)]
    for i, bar_date in enumerate(dates):
        idx = count - i - 1
        price = base_price * (1 + 0.001 * idx)
        prices.append({
            "xymd": bar_date.strftime("%Y%m%d"),
            "clos": f"{price:.2f}",
            "open": f"{price * 0.99:.2f}",
            "high": f"{price * 1.01:.2f}",
            "low": f"{price * 0.98:.2f}",
            "tvol": "1000000",
            "symbol": symbol,
        })
    # 반드시 오름차순 반환 (전략 코드가 closes[-1]을 최신가로 가정)
    return sorted(prices, key=lambda r: str(r.get("xymd", "")))


def _make_stub_price(symbol: str) -> dict:
    """offline/test 용 stub current price."""
    return {
        "last": "100.00",
        "open": "99.00",
        "high": "101.00",
        "low": "98.00",
        "tvol": "1000000",
        "symbol": symbol,
    }


def _safe_int(val: Any, default: int = 0) -> int:
    """숫자/문자열을 안전하게 int로 변환 (콤마, None, 빈 문자열 처리)."""
    if val is None or val == "" or val == "-":
        return default
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    if isinstance(val, str):
        cleaned = val.replace(",", "").strip()
        if not cleaned or cleaned == "-":
            return default
        try:
            return int(float(cleaned))
        except (ValueError, TypeError):
            return default
    return default


def _safe_float(val: Any, default: float = 0.0) -> float:
    """숫자/문자열을 안전하게 float로 변환 (콤마, None, 빈 문자열 처리)."""
    if val is None or val == "" or val == "-":
        return default
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        cleaned = val.replace(",", "").strip()
        if not cleaned or cleaned == "-":
            return default
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return default
    return default


def _get_first_valid(row: dict, keys: tuple[str, ...], default: Any = None, *, positive_numeric: bool = False) -> Any:
    """row에서 여러 key 후보 중 첫 번째로 존재하는 값을 반환.

    positive_numeric=True이면 None/blank/0은 결측으로 간주하고 다음 후보를 탐색한다.
    """
    for k in keys:
        if k in row:
            val = row[k]
            if val is None or val == "":
                continue
            if positive_numeric and _safe_float(val, 0.0) <= 0:
                continue
            return val
    return default


# KIS raw 필드 후보 정의
_CLOSE_KEYS = ("clos", "close", "stck_clpr", "ovrs_nmix_prpr", "prpr", "last", "price")
_OPEN_KEYS = ("open", "ovrs_nmix_oprc", "stck_oprc")
_HIGH_KEYS = ("high", "ovrs_nmix_hgpr", "stck_hgpr")
_LOW_KEYS = ("low", "ovrs_nmix_lwpr", "stck_lwpr")
_VOLUME_KEYS = (
    "volume", "vol", "tvol", "acml_vol",
    "cntg_vol", "trqu", "tvol_qty", "ovrs_vol",
)
_VALUE_KEYS = ("value", "amount", "acml_tr_pbmn")
_DATE_KEYS = ("xymd", "date", "stck_bsop_date", "bas_dt", "trad_dvsn")


def normalize_daily_row(row: dict) -> dict:
    """KIS raw 일봉 row를 표준 OHLCV dict로 정규화.

    close/open/high/low/volume/date 표준 필드를 보장한다.
    KIS raw 필드 후보를 모두 지원하며, 숫자는 comma 제거 후 float/int로 변환한다.
    """
    close_raw = _get_first_valid(row, _CLOSE_KEYS)
    open_raw = _get_first_valid(row, _OPEN_KEYS)
    high_raw = _get_first_valid(row, _HIGH_KEYS)
    low_raw = _get_first_valid(row, _LOW_KEYS)
    volume_raw = _get_first_valid(row, _VOLUME_KEYS, positive_numeric=True)
    value_raw = _get_first_valid(row, _VALUE_KEYS, positive_numeric=True)
    date_raw = _get_first_valid(row, _DATE_KEYS)

    close_val = _safe_float(close_raw) if close_raw is not None else None
    open_val = _safe_float(open_raw) if open_raw is not None else None
    high_val = _safe_float(high_raw) if high_raw is not None else None
    low_val = _safe_float(low_raw) if low_raw is not None else None
    volume_val = _safe_int(volume_raw) if volume_raw is not None else 0
    value_val = _safe_float(value_raw) if value_raw is not None else None

    result = dict(row)  # 원본 필드 보존 (기존 코드 호환)
    result["close"] = close_val
    result["open"] = open_val
    result["high"] = high_val
    result["low"] = low_val
    result["volume"] = volume_val
    if value_val is not None:
        result["value"] = value_val
        result["amount"] = value_val
    result["date"] = str(date_raw) if date_raw is not None else None
    # 하위 호환: clos / tvol 필드도 정규화 값으로 갱신
    if close_val is not None:
        result["clos"] = str(close_val)
    result["tvol"] = str(volume_val)
    return result


def normalize_daily_rows(symbol: str, rows: list[dict]) -> list[dict]:
    """일봉 row list를 정규화하고 필수 로그를 남긴다."""
    rows_raw = len(rows)
    if rows and symbol not in _DAILY_RAW_KEYS_LOGGED:
        _DAILY_RAW_KEYS_LOGGED.add(symbol)
        logger.info(
            "[US_DATA_PROVIDER][DAILY_RAW_KEYS] symbol=%s keys=%s",
            symbol,
            sorted(str(k) for k in rows[0].keys()),
        )
    normalized = [normalize_daily_row(r) for r in rows]
    rows_norm = len(normalized)
    close_nonnull = sum(1 for r in normalized if r.get("close") is not None and r["close"] > 0)
    volume_nonnull = sum(1 for r in normalized if r.get("volume") is not None and r["volume"] > 0)
    logger.info(
        "[US_DATA_PROVIDER][DAILY_NORMALIZE] symbol=%s rows_raw=%d rows_norm=%d"
        " close_nonnull=%d volume_nonnull=%d",
        symbol,
        rows_raw,
        rows_norm,
        close_nonnull,
        volume_nonnull,
    )
    return normalized


def normalize_us_balance(raw: dict) -> dict:
    """KIS 해외잔고 응답을 표준 positions 형태로 정규화.
    
    참고: 한국장 kis_wrapper._normalize_balance_snapshot 패턴을 미국장에 이식.
    
    Args:
        raw: KIS get_us_balance() raw response
    
    Returns:
        {
            "positions": [...],
            "total_pvs": str,
            "output1": [...],
            "output2": {...},
            "raw_balance": {...},
            "raw_output1_count": int,
            "normalized_position_count": int,
            "position_symbols": list[str],
            "balance_parse_status": "OK"|"ERROR",
            "balance_parse_error": str|None,
            "queried_exchanges": list[str],
            "exchange_result_counts": dict,
        }
    """
    result = {
        "positions": [],
        # Backward-compatible holdings market-value alias.  This is NOT broker
        # account equity and must never be used as an account/NAV denominator.
        "total_pvs": "0",
        "total_pvs_source": "none",
        "total_pvs_semantics": "holdings_market_value_usd",
        "holdings_market_value_usd": 0.0,
        "holdings_market_value_source": "none",
        "account_equity_usd": None,
        "account_equity_source": "unavailable_from_current_kis_balance_contract",
        "output1": [],
        "output2": {},
        "raw_balance": raw,
        "raw_output1_count": 0,
        "normalized_position_count": 0,
        "position_symbols": [],
        "balance_parse_status": "OK",
        "balance_parse_error": None,
        "queried_exchanges": raw.get("queried_exchanges", []),
        "exchange_result_counts": raw.get("exchange_result_counts", {}),
        "failed_exchanges": raw.get("failed_exchanges", {}),
        "balance_complete": raw.get("balance_complete", True),
        "balance_authoritative": raw.get("balance_authoritative", True),
    }
    
    # raw validation
    if not isinstance(raw, dict):
        result["balance_parse_status"] = "ERROR"
        result["balance_parse_error"] = "raw_not_dict"
        logger.error("[US_BALANCE][PARSE_ERROR] error=raw_not_dict type=%s", type(raw).__name__)
        return result
    
    # rt_cd check
    rt_cd = raw.get("rt_cd")
    if rt_cd is not None and str(rt_cd) != "0":
        result["balance_parse_status"] = "ERROR"
        result["balance_parse_error"] = f"rt_cd_not_zero rt_cd={rt_cd}"
        logger.error("[US_BALANCE][PARSE_ERROR] error=rt_cd_not_zero rt_cd=%s", rt_cd)
        return result
    
    # output1 normalize
    output1 = raw.get("output1")
    if output1 is None:
        output1 = []
    elif isinstance(output1, dict):
        output1 = [output1]
    elif not isinstance(output1, list):
        output1 = []
    
    # output2 normalize
    output2 = raw.get("output2")
    if output2 is None:
        output2 = {}
    elif isinstance(output2, list):
        output2 = output2[0] if output2 and isinstance(output2[0], dict) else {}
    elif not isinstance(output2, dict):
        output2 = {}
    
    result["output1"] = output1
    result["output2"] = output2
    result["raw_output1_count"] = len(output1)
    
    # Log raw structure
    logger.info(
        "[US_BALANCE][RAW] output1_count=%d output2_type=%s keys=%s",
        len(output1),
        type(output2).__name__,
        sorted(output2.keys()) if isinstance(output2, dict) else [],
    )
    
    # output1 → positions 변환
    positions = []
    for row in output1:
        if not isinstance(row, dict):
            continue
        
        # symbol
        symbol_candidates = ("ovrs_pdno", "pdno", "PDNO", "symbol", "item_cd", "prdt_code")
        symbol = _get_first_valid(row, symbol_candidates)
        if not symbol:
            continue
        symbol = str(symbol).strip().upper()
        if not symbol:
            continue
        
        # name
        name_candidates = ("ovrs_item_name", "prdt_name", "item_name", "hts_kor_isnm", "name")
        name = _get_first_valid(row, name_candidates, symbol)
        
        # exchange (raw → normalized)
        exchange_candidates = ("ovrs_excg_cd", "tr_mket_name", "exchange", "excd")
        raw_exchange = _get_first_valid(row, exchange_candidates, "NASD")
        raw_exchange_str = str(raw_exchange)
        
        # Normalize exchange to standard format (NASD → NASDAQ, etc.)
        from trader.us.symbols import normalize_us_exchange
        try:
            exchange = normalize_us_exchange(raw_exchange_str)
        except ValueError as exc:
            logger.warning(
                "[US_BALANCE][EXCHANGE_NORMALIZE_FAILED] symbol=%s raw_exchange=%s error=%s, defaulting to NASDAQ",
                symbol, raw_exchange_str, exc
            )
            exchange = "NASDAQ"
        
        # qty
        qty_candidates = ("ovrs_cblc_qty", "cblc_qty", "hldg_qty", "qty", "ord_psbl_qty")
        qty_raw = _get_first_valid(row, qty_candidates, "0")
        qty = _safe_int(qty_raw, 0)
        
        # qty <= 0 제외
        if qty <= 0:
            continue
        
        # orderable_qty
        orderable_qty_candidates = ("ord_psbl_qty", "sll_psbl_qty", "orderable_qty", "ovrs_cblc_qty")
        orderable_qty_raw = _get_first_valid(row, orderable_qty_candidates, str(qty))
        orderable_qty = _safe_int(orderable_qty_raw, qty)
        
        # avg_price_usd
        avg_price_candidates = ("pchs_avg_pric", "pchs_avg_price", "avg_price", "avg_price_usd")
        avg_price_raw = _get_first_valid(row, avg_price_candidates, "0")
        avg_price_usd = _safe_float(avg_price_raw, 0.0)
        
        # current_price_usd
        current_price_candidates = ("now_pric2", "ovrs_now_pric", "bass_pric", "last", "current_price")
        current_price_raw = _get_first_valid(row, current_price_candidates, "0")
        current_price_usd = _safe_float(current_price_raw, 0.0)
        
        # market_value_usd
        market_value_candidates = ("ovrs_stck_evlu_amt", "frcr_evlu_amt2", "evlu_amt", "market_value_usd")
        market_value_raw = _get_first_valid(row, market_value_candidates, "0")
        market_value_usd = _safe_float(market_value_raw, 0.0)
        
        # buy_amount_usd
        buy_amount_candidates = ("frcr_pchs_amt1", "pchs_amt", "buy_amount_usd")
        buy_amount_raw = _get_first_valid(row, buy_amount_candidates, "0")
        buy_amount_usd = _safe_float(buy_amount_raw, 0.0)
        
        # pnl_usd
        pnl_candidates = ("frcr_evlu_pfls_amt", "evlu_pfls_amt", "ovrs_stck_evlu_pfls_amt", "pnl_usd")
        pnl_raw = _get_first_valid(row, pnl_candidates, "0")
        pnl_usd = _safe_float(pnl_raw, 0.0)
        
        # pnl_rate
        pnl_rate_candidates = ("evlu_pfls_rt", "pnl_rate", "prls_rt")
        pnl_rate_raw = _get_first_valid(row, pnl_rate_candidates, "0")
        pnl_rate = _safe_float(pnl_rate_raw, 0.0)

        # exit contract 강화: entry_price alias
        # avg_price_usd가 0이면 buy_amount_usd / qty로 보완
        _resolved_avg_price = avg_price_usd
        _entry_price_source: str | None = None
        if _resolved_avg_price > 0:
            _entry_price_source = "kis_avg_price_usd"
        elif buy_amount_usd > 0 and qty > 0:
            _resolved_avg_price = buy_amount_usd / qty
            _entry_price_source = "kis_buy_amount_usd"

        # orderable_qty fallback 로그
        orderable_source = "kis_ord_psbl_qty"
        if orderable_qty == qty and not _get_first_valid(
            row,
            ("ord_psbl_qty", "sll_psbl_qty", "sellable_qty", "orderable_qty"),
        ):
            orderable_source = "qty_fallback"

        logger.info(
            "[US_BALANCE][POSITION] symbol=%s qty=%s orderable_qty=%s"
            " avg_price=%.4f current=%.4f source=%s",
            symbol,
            qty,
            orderable_qty,
            avg_price_usd,
            current_price_usd,
            orderable_source,
        )

        snapshot_asof = datetime.now(timezone.utc).isoformat()
        lifecycle_id = str(row.get("position_lifecycle_id") or row.get("lifecycle_id") or "").strip()
        positions.append({
            "symbol": symbol,
            "name": str(name),
            "exchange": exchange,  # Normalized (NASDAQ, NYSE, AMEX)
            "raw_exchange": raw_exchange_str,  # Preserve original KIS code
            "qty": qty,
            # KIS 매도가능수량 계열 — SELL qty guard 가 사용
            "holding_qty": qty,
            "orderable_qty": orderable_qty,
            "sellable_qty": orderable_qty,
            "avg_price_usd": avg_price_usd,
            "broker_avg_price": _resolved_avg_price if _resolved_avg_price > 0 else None,
            "broker_avg_price_source": "kis_pchs_avg_pric" if avg_price_usd > 0 else "kis_buy_amount_div_qty" if _resolved_avg_price > 0 else None,
            "broker_avg_price_currency": "USD",
            "broker_avg_price_asof": snapshot_asof,
            "authoritative_positions": True,
            "position_lifecycle_id": lifecycle_id or None,
            "current_price_usd": current_price_usd,
            "market_value_usd": market_value_usd,
            "buy_amount_usd": buy_amount_usd,
            "pnl_usd": pnl_usd,
            "pnl_rate": pnl_rate,
            # exit contract aliases
            "entry_price": _resolved_avg_price if _resolved_avg_price > 0 else None,
            "avg_cost": _resolved_avg_price if _resolved_avg_price > 0 else None,
            "current_px": current_price_usd if current_price_usd > 0 else None,
            "unrealized_pnl_usd": pnl_usd,
            "entry_price_source": _entry_price_source,
            "balance_source": "kis_balance_authoritative",
            "raw": row,
        })

    # Holdings market value only.  KIS overseas inquire-balance output2 fields
    # available in this production contract are PnL/return aggregates, not a
    # proven cash+securities account-equity field.  Keep the legacy total_pvs
    # alias for compatibility but make its semantics explicit.
    if positions:
        holdings_market_value = sum(p["market_value_usd"] for p in positions)
        result["total_pvs"] = str(holdings_market_value)
        result["total_pvs_source"] = "positions_market_value_sum"
        result["holdings_market_value_usd"] = holdings_market_value
        result["holdings_market_value_source"] = "positions_market_value_sum"
        logger.info(
            "[US_BALANCE][SUMMARY] total_pvs_source=%s total_pvs=%.2f semantics=holdings_market_value_usd positions=%d",
            result["total_pvs_source"],
            holdings_market_value,
            len(positions),
        )
    else:
        result["total_pvs"] = "0"
        result["total_pvs_source"] = "zero_no_positions"
        result["holdings_market_value_usd"] = 0.0
        result["holdings_market_value_source"] = "zero_no_positions"
        logger.info("[US_BALANCE][SUMMARY] total_pvs_source=zero_no_positions semantics=holdings_market_value_usd")
    
    # pnl_usd 별도 추출
    pnl_candidates = (
        "tot_evlu_pfls_amt",    # 총평가손익금액
        "frcr_evlu_pfls_amt",   # 외화평가손익금액
        "tot_pfls_amt",         # 총손익금액
    )
    pnl_val = _get_first_valid(output2, pnl_candidates, "0")
    result["pnl_usd"] = str(_safe_float(pnl_val, 0.0))
    
    result["positions"] = positions
    result["normalized_position_count"] = len(positions)
    result["position_symbols"] = [p["symbol"] for p in positions]
    
    # Log normalized result
    if positions:
        logger.info(
            "[US_BALANCE][NORMALIZED] positions=%d symbols=%s",
            len(positions),
            ",".join(result["position_symbols"]),
        )
    else:
        logger.info("[US_BALANCE][NORMALIZED] positions=0")
    
    # Contract error: raw_output1_count > 0 but normalized_position_count == 0
    if result["raw_output1_count"] > 0 and result["normalized_position_count"] == 0:
        result["balance_parse_status"] = "CONTRACT_ERROR"
        result["balance_parse_error"] = "raw_output1_nonzero_positions_zero"
        logger.error(
            "[US_BALANCE][CONTRACT_ERROR] raw_output1_count=%d normalized_position_count=0",
            result["raw_output1_count"],
        )
    
    return result

def normalize_us_order_status_row(row: dict) -> dict:
    """Normalize KIS US order/fill status fields for timeout replay."""
    order_no = str(_get_first_valid(row, ("order_no", "odno", "ODNO"), "") or "").strip()
    symbol = str(_get_first_valid(row, ("symbol", "pdno", "PDNO"), "") or "").strip().upper()
    side_raw = str(_get_first_valid(row, ("side", "sll_buy_dvsn_cd", "SLL_BUY_DVSN_CD"), "") or "").upper()
    side = "BUY" if side_raw in {"BUY", "02", "B"} else "SELL" if side_raw in {"SELL", "01", "S"} else side_raw
    requested = _safe_int(_get_first_valid(row, ("requested_qty", "qty", "ord_qty", "ft_ord_qty", "ORD_QTY"), 0))
    filled = _safe_int(_get_first_valid(row, ("filled_qty", "ft_ccld_qty", "ccld_qty", "tot_ccld_qty"), 0))
    remaining = _safe_int(_get_first_valid(row, ("remaining_qty", "nccs_qty", "rmn_qty"), max(0, requested - filled)))
    raw_status = str(_get_first_valid(row, ("status", "ord_dvsn_name", "ord_sttus", "rjct_rson"), "") or "").upper()
    if "REJECT" in raw_status or "거부" in raw_status:
        status = "REJECTED"
    elif "CANCEL" in raw_status or "취소" in raw_status:
        status = "CANCELLED"
    elif requested and filled >= requested:
        status = "FILLED"
    elif filled > 0:
        status = "PARTIALLY_FILLED"
    elif requested > 0 and remaining > 0:
        status = "OPEN"
    elif order_no:
        status = "ACK_PENDING"
    else:
        status = "UNKNOWN"
    trade_date_raw = str(_get_first_valid(row, ("trade_date", "ord_dt", "ORD_DT"), "") or "")
    order_time_raw = str(_get_first_valid(row, ("ord_tmd", "ORD_TMD"), "") or "")
    submitted_at_utc = _get_first_valid(row, ("submitted_at_utc", "order_timestamp", "observed_at"), None)
    time_error = None
    if not submitted_at_utc and trade_date_raw and order_time_raw:
        try:
            from zoneinfo import ZoneInfo
            local = datetime.strptime(trade_date_raw.replace("-", "") + order_time_raw, "%Y%m%d%H%M%S").replace(tzinfo=ZoneInfo("America/New_York"))
            submitted_at_utc = local.astimezone(timezone.utc).isoformat()
        except Exception:
            time_error = "invalid_kis_order_datetime"
    normalization_result = "normalized" if order_no and symbol and side in {"BUY", "SELL"} and requested > 0 and not time_error else "quarantined"
    filter_reason = None if normalization_result == "normalized" else time_error or "required_order_schema_missing"
    return {
        "order_no": order_no, "raw_order_no": order_no,
        "canonical_order_no": normalize_us_order_no(order_no), "symbol": symbol, "side": side,
        "requested_qty": requested, "filled_qty": filled, "remaining_qty": remaining,
        "status": status,
        "trade_date": trade_date_raw,
        "exchange": str(_get_first_valid(row, ("exchange", "ovrs_excg_cd", "OVRS_EXCG_CD"), "") or ""),
        "limit_price": _safe_float(_get_first_valid(row, ("limit_price", "ft_ord_unpr3", "ord_unpr"), 0.0)),
        "submitted_at_utc": submitted_at_utc,
        "original_order_no": str(_get_first_valid(row, ("orgn_odno", "original_order_no"), "") or ""),
        "raw_row_id": row.get("raw_row_id"), "page_index": row.get("page_index", 0),
        "normalization_result": normalization_result, "filter_reason": filter_reason,
        "avg_price": _safe_float(_get_first_valid(row, ("avg_price", "ft_ccld_unpr3", "avg_prvs"), 0.0)),
        "raw": row,
    }


class USDataProvider:
    """미국주식 데이터 제공자.

    Args:
        offline: True이면 stub 데이터만 반환 (HTTP 호출 없음)
        cache_enabled: True이면 prep run 내에서 daily/price 캐시 사용
    """

    def __init__(self, offline: bool = False, cache_enabled: bool = False) -> None:
        try:
            from trader.us.execution.kis_us_client import kis_http_block_enabled
            offline = bool(offline or kis_http_block_enabled())
        except Exception:
            offline = bool(offline)
        self._offline = offline
        self._cache_enabled = cache_enabled
        self._client = None
        self._daily_cache: dict = {}
        self._price_cache: dict = {}
        self._tick_context = None
        self.stats = {
            "daily_hit": 0,
            "daily_miss": 0,
            "daily_ok_symbols": set(),
            "daily_fail_symbols": set(),
            "price_hit": 0,
            "price_miss": 0,
            "price_ok_symbols": set(),
            "price_fail_symbols": set(),
            "fail_reasons": {},
            "daily_http_call_count": 0,
        }

    def bind_tick_context(self, context: Any) -> "USDataProvider":
        self._tick_context = context
        if self._client is not None and hasattr(self._client, "bind_tick_context"):
            self._client.bind_tick_context(context)
        return self

    def _get_client(self):
        if self._client is None:
            from trader.us.execution.kis_us_client import KisUSClient
            self._client = KisUSClient(env="practice", offline=self._offline)
        if self._tick_context is not None and hasattr(self._client, "bind_tick_context"):
            self._client.bind_tick_context(self._tick_context)
        return self._client

    def _load_daily_prices_from_db(
        self, symbol: str, market: str = "US", days: int = 120
    ) -> list[dict]:
        """DB price_daily에서 일봉 조회 (fallback 용)."""
        try:
            engine = _get_engine()
            if engine is None:
                return []
            
            with engine.begin() as conn:
                cutoff_date = date.today() - timedelta(days=days)
                query = text(
                    """
                    SELECT date, open, high, low, close, volume
                    FROM price_daily
                    WHERE market = :market AND code = :code AND date >= :cutoff
                    ORDER BY date ASC
                    """
                )
                rows = conn.execute(
                    query, {"market": market, "code": symbol, "cutoff": cutoff_date}
                ).fetchall()
                
                result = []
                for row in rows:
                    result.append({
                        "xymd": row[0].strftime("%Y%m%d"),
                        "clos": str(row[4]) if row[4] else "0",
                        "open": str(row[1]) if row[1] else "0",
                        "high": str(row[2]) if row[2] else "0",
                        "low": str(row[3]) if row[3] else "0",
                        "tvol": str(row[5]) if row[5] else "0",
                        "symbol": symbol,
                    })
                return result
        except Exception as exc:
            logger.warning(
                "[US_DATA][FALLBACK_DB_FAIL] symbol=%s error=%s", symbol, exc
            )
            return []

    def _load_latest_price_from_db(
        self, symbol: str, market: str = "US", max_age_days: int = 3
    ) -> dict | None:
        """DB price_daily에서 최근 가격 조회 (stale fallback 용)."""
        try:
            engine = _get_engine()
            if engine is None:
                return None
            
            with engine.begin() as conn:
                cutoff_date = date.today() - timedelta(days=max_age_days)
                query = text(
                    """
                    SELECT date, open, high, low, close, volume
                    FROM price_daily
                    WHERE market = :market AND code = :code AND date >= :cutoff
                    ORDER BY date DESC
                    LIMIT 1
                    """
                )
                row = conn.execute(
                    query, {"market": market, "code": symbol, "cutoff": cutoff_date}
                ).fetchone()
                
                if not row:
                    return None
                
                return {
                    "last": str(row[4]) if row[4] else "0",
                    "open": str(row[1]) if row[1] else "0",
                    "high": str(row[2]) if row[2] else "0",
                    "low": str(row[3]) if row[3] else "0",
                    "tvol": str(row[5]) if row[5] else "0",
                    "symbol": symbol,
                    "_stale_date": row[0].strftime("%Y-%m-%d"),
                    "stale": True,
                    "quality": "stale",
                    "source": "DB_STALE",
                    "asof": row[0].strftime("%Y-%m-%d"),
                }
        except Exception as exc:
            logger.warning(
                "[US_DATA][FALLBACK_STALE_FAIL] symbol=%s error=%s", symbol, exc
            )
            return None

    def get_current_price(self, symbol: str, exchange: str) -> dict:
        """현재가 조회 (KIS → DB stale fallback)."""
        cache_key = (symbol.upper(), exchange.upper())
        ctx = self._tick_context
        if ctx is not None:
            ctx.count("quote_logical_calls")
            if cache_key in ctx.price_cache:
                ctx.count("quote_cache_hits")
                return ctx.price_cache[cache_key]
        started = __import__("time").monotonic()
        
        # Cache hit
        if self._cache_enabled and cache_key in self._price_cache:
            self.stats["price_hit"] += 1
            logger.debug("[US_DATA][CACHE_HIT] type=price symbol=%s", symbol)
            return self._price_cache[cache_key]
        
        # Cache miss
        if self._cache_enabled:
            self.stats["price_miss"] += 1
            logger.debug("[US_DATA][CACHE_MISS] type=price symbol=%s", symbol)
        
        if self._offline:
            logger.debug("[US_DATA][OFFLINE] current_price symbol=%s", symbol)
            result = _make_stub_price(symbol)
            if self._cache_enabled:
                self._price_cache[cache_key] = result
                self.stats["price_ok_symbols"].add(symbol.upper())
            if ctx is not None:
                ctx.price_cache[cache_key] = result
            return result
        
        try:
            if ctx is not None:
                ctx.count("quote_http_calls")
            result = self._get_client().get_us_price(symbol, exchange)
            output = result.get("output", {})
            quote_quality = str(result.get("_quote_quality") or "FRESH").upper()
            data = {
                "last": output.get("last", "0"),
                "open": output.get("open", "0"),
                "high": output.get("high", "0"),
                "low": output.get("low", "0"),
                "tvol": output.get("tvol", "0"),
                "symbol": symbol,
                "stale": quote_quality in {"STALE", "DEGRADED", "SUSPECT"},
                "quality": quote_quality.lower(),
                "source": str(result.get("_quote_source") or "KIS_LIVE"),
                "asof_epoch": result.get("_quote_asof_epoch"),
                "age_sec": result.get("_quote_age_sec"),
            }
            if self._cache_enabled:
                self._price_cache[cache_key] = data
                self.stats["price_ok_symbols"].add(symbol.upper())
            if ctx is not None:
                ctx.price_cache[cache_key] = data
            return data
        except Exception as exc:
            from trader.us.execution.kis_us_client import KisUSTemporaryError
            
            if isinstance(exc, KisUSTemporaryError):
                logger.warning(
                    "[US_DATA][FALLBACK_STALE] symbol=%s KIS failed, trying DB: %s",
                    symbol, exc
                )
                stale_data = self._load_latest_price_from_db(symbol, market="US", max_age_days=3)
                if stale_data:
                    logger.info(
                        "[US_DATA][FALLBACK_STALE_OK] symbol=%s date=%s",
                        symbol, stale_data.get("_stale_date", "unknown")
                    )
                    if self._cache_enabled:
                        self._price_cache[cache_key] = stale_data
                        self.stats["price_ok_symbols"].add(symbol.upper())
                    return stale_data
                else:
                    logger.error(
                        "[US_DATA][FALLBACK_STALE_EMPTY] symbol=%s no DB data", symbol
                    )
                    if self._cache_enabled:
                        self.stats["price_fail_symbols"].add(symbol.upper())
                        self.stats["fail_reasons"][symbol.upper()] = "KIS_TEMP_ERROR_NO_DB_FALLBACK"
            else:
                # Non-temporary error
                if self._cache_enabled:
                    self.stats["price_fail_symbols"].add(symbol.upper())
                    self.stats["fail_reasons"][symbol.upper()] = str(type(exc).__name__)
            # Re-raise original exception if not temporary or no DB fallback
            if ctx is not None:
                ctx.count("quote_retry_calls")
            raise
        finally:
            if ctx is not None:
                ctx.metrics["price_fetch_ms"] = float(ctx.metrics.get("price_fetch_ms", 0.0)) + (__import__("time").monotonic() - started) * 1000.0


    def get_completed_daily_prices_result(
        self,
        symbol: str,
        exchange: str,
        *,
        trade_date: str,
        required_bars: int = 260,
        allow_http_sync: bool,
    ) -> dict:
        """DB-first completed US daily bars with structured quality metadata."""
        from trader.us.db.price_daily_repo import (
            audit_us_daily_history,
            get_latest_us_daily_date,
            load_recent_us_daily_bars,
            upsert_us_daily_bars,
        )
        from trader.us.market_calendar import previous_completed_us_session
        required = int(required_bars or int(os.getenv("US_DAILY_REQUIRED_BARS", "260")))
        try:
            rows = load_recent_us_daily_bars(symbol=symbol, before_date=trade_date, limit=required)
            audit = audit_us_daily_history(symbol=symbol, before_date=trade_date, required_bars=required)
        except Exception as exc:
            logger.error("[US_OHLCV][DB_ERROR] symbol=%s code=US_DAILY_DB_UNAVAILABLE err=%s", symbol, exc)
            return {"rows": [], "quality": "DB_ERROR", "valid_bar_count": 0, "db_latest": None, "expected_latest": None, "invalid_close_count": 0, "duplicate_count": 0, "http_sync_attempted": False, "http_sync_succeeded": False}
        expected_latest = previous_completed_us_session(trade_date)
        db_latest = get_latest_us_daily_date(symbol=symbol, before_date=trade_date)
        from trader.us.dates import canonical_us_bar_date
        audit_last_date = canonical_us_bar_date(audit.get("last_date"))
        invalid_close_count = int(audit.get("invalid_close_count") or 0)
        duplicate_count = int(audit.get("duplicate_count") or 0)
        if len(rows) >= required and audit_last_date == expected_latest and invalid_close_count == 0 and duplicate_count == 0:
            logger.info("[US_OHLCV][DB_HIT] symbol=%s required=%d loaded=%d latest=%s expected=%s kis_calls=0", symbol, required, len(rows), audit.get("last_date"), expected_latest)
            return {"rows": normalize_daily_rows(symbol, rows), "quality": "OK", "valid_bar_count": len(rows), "db_latest": db_latest.isoformat() if db_latest else None, "expected_latest": expected_latest.isoformat(), "invalid_close_count": invalid_close_count, "duplicate_count": duplicate_count, "http_sync_attempted": False, "http_sync_succeeded": False}
        if not allow_http_sync or os.getenv("US_DAILY_SYNC_ENABLED", "1") in {"0", "false", "False", "no"}:
            logger.info("[US_OHLCV][DB_HIT] symbol=%s required=%d loaded=%d latest=%s expected=%s kis_calls=0 quality=INSUFFICIENT_OR_STALE", symbol, required, len(rows), audit.get("last_date"), expected_latest)
            return {"rows": normalize_daily_rows(symbol, rows), "quality": ("STALE" if audit_last_date != expected_latest else "INSUFFICIENT_HISTORY"), "valid_bar_count": len(rows), "db_latest": db_latest.isoformat() if db_latest else None, "expected_latest": expected_latest.isoformat(), "invalid_close_count": invalid_close_count, "duplicate_count": duplicate_count, "http_sync_attempted": False, "http_sync_succeeded": False}
        max_pages = int(os.getenv("US_DAILY_BACKFILL_MAX_PAGES", "6") or 6)
        client = None if self._offline else self._get_client()

        def _fetch_history(*, as_of: str, need: int, stop_at: str | None, direction: str) -> list[dict]:
            if self._offline:
                return _make_stub_daily(symbol, max(required + 5, need + 5))
            if hasattr(client, "get_us_daily_price_history"):
                self.stats["daily_http_call_count"] += 1
                return client.get_us_daily_price_history(symbol, exchange, as_of_date=as_of, required_bars=need, stop_at_date=stop_at, max_pages=max_pages)
            self.stats["daily_http_call_count"] += 1
            return client.get_us_daily_price(symbol, exchange, need, as_of_date=as_of)

        total_fetched = 0
        total_upserted = 0
        if db_latest is None or db_latest < expected_latest:
            logger.info("[US_OHLCV][SYNC_GAP] symbol=%s direction=forward db_latest=%s expected_latest=%s", symbol, db_latest, expected_latest)
            fetched = _fetch_history(as_of=str(trade_date), need=required, stop_at=(db_latest.isoformat() if db_latest else None), direction="forward")
            total_fetched += len(fetched or [])
            total_upserted += upsert_us_daily_bars(symbol=symbol, bars=fetched, source="KIS_US_DAILY")
            rows = load_recent_us_daily_bars(symbol=symbol, before_date=trade_date, limit=required)
            db_latest = get_latest_us_daily_date(symbol=symbol, before_date=trade_date)

        if db_latest == expected_latest and len(rows) < required:
            from trader.us.dates import canonical_us_bar_date
            from datetime import timedelta as _td
            oldest = canonical_us_bar_date((rows[0] or {}).get("date") or (rows[0] or {}).get("xymd")) if rows else expected_latest
            as_of_back = (oldest - _td(days=1)).strftime("%Y%m%d") if oldest else str(trade_date)
            missing = max(required - len(rows), 1)
            logger.info("[US_OHLCV][SYNC_GAP] symbol=%s direction=backward oldest=%s missing=%d", symbol, oldest, missing)
            fetched = _fetch_history(as_of=as_of_back, need=max(missing, required), stop_at=None, direction="backward")
            total_fetched += len(fetched or [])
            total_upserted += upsert_us_daily_bars(symbol=symbol, bars=fetched, source="KIS_US_DAILY")

        pages = getattr(client, "last_daily_pages", None) if client is not None else None
        logger.info("[US_OHLCV][BACKFILL] symbol=%s before_count=%d target=%d fetched=%d pages=%s", symbol, len(rows), required, total_fetched, pages)
        logger.info("[US_OHLCV][UPSERT] symbol=%s fetched=%d upserted=%d", symbol, total_fetched, total_upserted)
        rows = load_recent_us_daily_bars(symbol=symbol, before_date=trade_date, limit=required)
        audit = audit_us_daily_history(symbol=symbol, before_date=trade_date, required_bars=required)
        db_latest = get_latest_us_daily_date(symbol=symbol, before_date=trade_date)
        audit_last_date = canonical_us_bar_date(audit.get("last_date"))
        invalid_close_count = int(audit.get("invalid_close_count") or 0)
        duplicate_count = int(audit.get("duplicate_count") or 0)
        quality = "OK" if len(rows) >= required and audit_last_date == expected_latest and invalid_close_count == 0 and duplicate_count == 0 else ("STALE" if audit_last_date != expected_latest else "INSUFFICIENT_HISTORY")
        logger.info("[US_OHLCV][VERIFY] symbol=%s quality=%s valid_bar_count=%d latest=%s expected=%s invalid_close_count=%s duplicate_count=%s", symbol, quality, len(rows), db_latest, expected_latest, audit.get("invalid_close_count"), audit.get("duplicate_count"))
        return {"rows": normalize_daily_rows(symbol, rows), "quality": quality, "valid_bar_count": len(rows), "db_latest": db_latest.isoformat() if db_latest else None, "expected_latest": expected_latest.isoformat(), "invalid_close_count": invalid_close_count, "duplicate_count": duplicate_count, "http_sync_attempted": total_fetched > 0, "http_sync_succeeded": total_fetched == 0 or total_upserted > 0}

    def get_completed_daily_prices(
        self,
        symbol: str,
        exchange: str,
        *,
        trade_date: str,
        required_bars: int = 260,
        allow_http_sync: bool,
    ) -> list[dict]:
        return self.get_completed_daily_prices_result(
            symbol, exchange, trade_date=trade_date, required_bars=required_bars, allow_http_sync=allow_http_sync
        ).get("rows", [])

    def get_daily_prices(self, symbol: str, exchange: str, count: int = 120, as_of_date: str | None = None) -> list[dict]:
        """일봉 데이터 조회 (xymd 기준 오름차순 정렬, KIS → DB fallback).

        전략 코드가 closes[-1]을 최신 가격으로 가정하므로 반드시 오름차순 반환.
        as_of_date가 있으면 KIS BYMD에 해당 날짜를 사용한다 (force_now 지원).
        """
        logger.info(
            "[US_DATA_PROVIDER][GET_DAILY_PRICES] symbol=%s exchange=%s count=%s as_of_date=%s",
            symbol,
            exchange,
            count,
            as_of_date,
        )
        cache_key = (symbol.upper(), exchange.upper(), int(count))
        
        # Cache hit
        if self._cache_enabled and cache_key in self._daily_cache:
            self.stats["daily_hit"] += 1
            logger.debug("[US_DATA][CACHE_HIT] type=daily symbol=%s count=%d", symbol, count)
            return self._daily_cache[cache_key]
        
        # Cache miss
        if self._cache_enabled:
            self.stats["daily_miss"] += 1
            logger.debug("[US_DATA][CACHE_MISS] type=daily symbol=%s count=%d", symbol, count)
        
        if self._offline:
            logger.debug("[US_DATA][OFFLINE] daily_prices symbol=%s count=%d", symbol, count)
            raw_stub = _make_stub_daily(symbol, count)
            result = normalize_daily_rows(symbol, raw_stub)
            if self._cache_enabled:
                self._daily_cache[cache_key] = result
                self.stats["daily_ok_symbols"].add(symbol.upper())
            return result
        
        try:
            rows = self._get_client().get_us_daily_price(symbol, exchange, count, as_of_date=as_of_date)
            normalized = normalize_daily_rows(symbol, rows)
            result = sorted(normalized, key=lambda r: str(r.get("xymd", "") or r.get("date", "")))
            if self._cache_enabled:
                self._daily_cache[cache_key] = result
                self.stats["daily_ok_symbols"].add(symbol.upper())
            return result
        except Exception as exc:
            from trader.us.execution.kis_us_client import KisUSTemporaryError
            
            if isinstance(exc, KisUSTemporaryError):
                logger.warning(
                    "[US_DATA][FALLBACK_DB] symbol=%s KIS failed, trying DB: %s",
                    symbol, exc
                )
                db_rows = self._load_daily_prices_from_db(symbol, market="US", days=count)
                if db_rows:
                    normalized_db = normalize_daily_rows(symbol, db_rows)
                    logger.info(
                        "[US_DATA][FALLBACK_DB_OK] symbol=%s count=%d", symbol, len(normalized_db)
                    )
                    if self._cache_enabled:
                        self._daily_cache[cache_key] = normalized_db
                        self.stats["daily_ok_symbols"].add(symbol.upper())
                    return normalized_db
                else:
                    logger.error(
                        "[US_DATA][FALLBACK_DB_EMPTY] symbol=%s no DB data", symbol
                    )
                    if self._cache_enabled:
                        self.stats["daily_fail_symbols"].add(symbol.upper())
                        self.stats["fail_reasons"][symbol.upper()] = "KIS_TEMP_ERROR_NO_DB_FALLBACK"
            else:
                # Non-temporary error
                if self._cache_enabled:
                    self.stats["daily_fail_symbols"].add(symbol.upper())
                    self.stats["fail_reasons"][symbol.upper()] = str(type(exc).__name__)
            # Re-raise original exception if not temporary or no DB fallback
            raise

    def get_balance(self, force_refresh: bool = False) -> dict:
        """잔고 조회."""
        ctx = self._tick_context
        if ctx is not None:
            ctx.count("balance_logical_calls")
            if ctx.balance_snapshot_at is not None and ctx.balance_snapshot:
                ctx.count("balance_cache_hits")
                return ctx.balance_snapshot
        started = __import__("time").monotonic()
        if self._offline:
            return {
                "total_pvs": "10000.00",
                "frcr_pchs_amt1": "5000.00",
                "ovrs_tot_pfls": "500.00",
                "positions": [],
                "output1": [],
                "output2": {},
                "raw_balance": {},
                "raw_output1_count": 0,
                "normalized_position_count": 0,
                "position_symbols": [],
                "balance_parse_status": "OK",
                "balance_parse_error": None,
                "queried_exchanges": [],
                "exchange_result_counts": {},
            }
        try:
            if ctx is not None:
                ctx.count("balance_http_calls")
            raw = self._get_client().get_us_balance(force_refresh=force_refresh)
            result = normalize_us_balance(raw)
            if ctx is not None and result.get("balance_complete", True):
                ctx.balance_snapshot = result
                ctx.balance_snapshot_at = __import__("time").monotonic()
            return result
        except Exception:
            if ctx is not None:
                ctx.count("balance_retry_calls")
            raise
        finally:
            if ctx is not None:
                ctx.metrics["balance_snapshot_ms"] = float(ctx.metrics.get("balance_snapshot_ms", 0.0)) + (__import__("time").monotonic() - started) * 1000.0

    def get_orderable_cash(
        self,
        symbol: str = "AAPL",
        exchange: str = "NASDAQ",
        price: float = 100.0,
    ) -> float:
        """주문 가능 현금 (USD).

        Args:
            symbol:   종목 코드 (KIS 모의투자 필수 파라미터).
            exchange: 거래소. KIS API OVRS_EXCG_CD 로 변환.
            price:    호가 (OVRS_ORD_UNPR). KIS API 필수 파라미터.

        후보 필드 (KIS 환경에 따라 다를 수 있음):
        frcr_ord_psbl_amt1, ord_psbl_cash, ovrs_ord_psbl_amt,
        orderable_cash, cash, psbl_amt
        """
        ctx = self._tick_context
        cache_key = (symbol.upper(), exchange.upper(), round(float(price), 6), "BUY")
        if ctx is not None:
            ctx.count("psamount_logical_calls")
            if cache_key in ctx.psamount_cache:
                ctx.count("psamount_cache_hits")
                return float(ctx.psamount_cache[cache_key])
        started = __import__("time").monotonic()
        if self._offline:
            return 1000.0
        try:
            if ctx is not None:
                ctx.count("psamount_http_calls")
            raw = self._get_client().get_us_orderable_cash(
                symbol=symbol, exchange=exchange, price=price
            )
        except Exception as exc:
            if ctx is not None:
                ctx.count("psamount_retry_calls")
            logger.warning("[US_DATA][WARN] get_orderable_cash API failed: %s", exc)
            return 0.0
        output = raw.get("output", raw)  # output 없으면 raw 자체 시도
        candidates = (
            "frcr_ord_psbl_amt1", "ord_psbl_cash", "ovrs_ord_psbl_amt",
            "orderable_cash", "cash", "psbl_amt",
        )
        for key in candidates:
            val = output.get(key)
            if val is not None:
                try:
                    result = float(val)
                    if ctx is not None:
                        ctx.psamount_cache[cache_key] = result
                        ctx.metrics["psamount_ms"] = float(ctx.metrics.get("psamount_ms", 0.0)) + (__import__("time").monotonic() - started) * 1000.0
                    return result
                except (ValueError, TypeError):
                    logger.warning("[US_DATA][WARN] orderable_cash field %s not numeric: %s", key, val)
        logger.warning("[US_DATA][WARN] orderable_cash not found in response, returning 0.0")
        return 0.0
    

    def get_today_orders(self, trade_date: str) -> list[dict]:
        """Return normalized same-day US broker order status rows."""
        if self._offline:
            return []
        client = self._get_client()
        method = getattr(client, "get_us_today_orders", None)
        if not callable(method):
            # KIS order/fill endpoint exposes order status in ccnl rows on many schemas.
            raw = client.get_us_fills_today(trade_date=trade_date)
        else:
            raw = method(trade_date=trade_date)
        normalized = [normalize_us_order_status_row(row) for row in (raw or [])]
        quarantined = sum(row["normalization_result"] == "quarantined" for row in normalized)
        valid = len(normalized) - quarantined
        logger.info("[ORDER_NORMALIZATION] raw_count=%d normalized_count=%d ignored_count=0 quarantined_count=%d", len(raw or []), valid, quarantined)
        for row in normalized:
            logger.info("[ORDER_NORMALIZATION][ROW] raw_row_id=%s page_index=%s raw_order_no=%s canonical_order_no=%s symbol=%s side=%s requested_qty=%s filled_qty=%s remaining_qty=%s normalization_result=%s filter_reason=%s",
                        row.get("raw_row_id"), row.get("page_index"), row.get("raw_order_no"), row.get("canonical_order_no"), row.get("symbol"), row.get("side"), row.get("requested_qty"), row.get("filled_qty"), row.get("remaining_qty"), row.get("normalization_result"), row.get("filter_reason"))
        return normalized

    def get_fills_by_order_no(self, order_no: str, symbol: str, trade_date: str) -> dict | None:
        """Return normalized cumulative fill/order detail for a single broker order."""
        if self._offline:
            return None
        rows = self.get_today_orders(trade_date)
        wanted = normalize_us_order_no(order_no)
        matches = [r for r in rows if normalize_us_order_no(r.get("order_no")) == wanted and str(r.get("symbol") or "").upper() == str(symbol).upper()]
        if not matches:
            return None
        def _observed(row: dict) -> str:
            return str(row.get("observed_at") or row.get("updated_at") or row.get("order_timestamp") or row.get("order_time") or "")
        best = matches[0]
        conflict = False
        for row in matches[1:]:
            row_qty = _safe_int(row.get("filled_qty") or row.get("cumulative_filled_qty") or 0)
            best_qty = _safe_int(best.get("filled_qty") or best.get("cumulative_filled_qty") or 0)
            if row_qty > best_qty or (row_qty == best_qty and _observed(row) >= _observed(best)):
                best = row
            elif _observed(row) > _observed(best) and row_qty < best_qty:
                conflict = True
        out = dict(best)
        out["filled_qty"] = _safe_int(best.get("filled_qty") or best.get("cumulative_filled_qty") or 0)
        out["cumulative_filled_qty"] = out["filled_qty"]
        out["avg_price"] = _safe_float(best.get("avg_price") or best.get("avg_price_usd") or 0.0)
        if conflict:
            out["status"] = "EVIDENCE_QUANTITY_REGRESSION"
            out["requires_reconcile"] = True
        return out

    def get_client_stats(self) -> dict:
        """KIS client stats 반환 (retry count 등)."""
        if self._client is None:
            return {}
        return getattr(self._client, "stats", {})
