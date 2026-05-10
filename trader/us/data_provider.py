# -*- coding: utf-8 -*-
"""미국주식 Data Provider.

offline 모드에서는 fixture 데이터를 반환한다.
online 모드에서는 KisUSClient를 통해 실제 데이터를 가져온다.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)


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
    d = date.today()
    for i in range(count):
        idx = count - i - 1
        price = base_price * (1 + 0.001 * idx)
        prices.append({
            "xymd": (d - timedelta(days=idx)).strftime("%Y%m%d"),
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


def _get_first_valid(row: dict, keys: tuple[str, ...], default: Any = None) -> Any:
    """row에서 여러 key 후보 중 첫 번째로 존재하는 값을 반환."""
    for k in keys:
        if k in row:
            val = row[k]
            if val is not None and val != "":
                return val
    return default


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
        "total_pvs": "0",
        "total_pvs_source": "none",
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
        
        positions.append({
            "symbol": symbol,
            "name": str(name),
            "exchange": exchange,  # Normalized (NASDAQ, NYSE, AMEX)
            "raw_exchange": raw_exchange_str,  # Preserve original KIS code
            "qty": qty,
            "orderable_qty": orderable_qty,
            "avg_price_usd": avg_price_usd,
            "current_price_usd": current_price_usd,
            "market_value_usd": market_value_usd,
            "buy_amount_usd": buy_amount_usd,
            "pnl_usd": pnl_usd,
            "pnl_rate": pnl_rate,
            "raw": row,
        })

    # total_pvs = 총 평가금액 (evaluation amount, not pnl)
    # output2의 ovrs_tot_pfls, tot_evlu_pfls_amt는 손익(pnl)이므로 사용하지 않음
    # 항상 positions의 market_value_usd 합계 사용
    if positions:
        total_pvs_calculated = sum(p["market_value_usd"] for p in positions)
        result["total_pvs"] = str(total_pvs_calculated)
        result["total_pvs_source"] = "positions_market_value_sum"
        logger.info(
            "[US_BALANCE][SUMMARY] total_pvs_source=%s total_pvs=%.2f positions=%d",
            result["total_pvs_source"],
            total_pvs_calculated,
            len(positions),
        )
    elif output2:
        # positions가 없으면 buy_amount 합계 fallback (legacy)
        buy_amount_fallback = _safe_float(output2.get("tot_apl_amt", "0"), 0.0)
        if buy_amount_fallback > 0:
            result["total_pvs"] = str(buy_amount_fallback)
            result["total_pvs_source"] = "output2_buy_amount_fallback"
            logger.warning(
                "[US_BALANCE][SUMMARY] total_pvs_source=%s total_pvs=%.2f (no positions)",
                result["total_pvs_source"],
                buy_amount_fallback,
            )
        else:
            result["total_pvs"] = "0"
            result["total_pvs_source"] = "zero_no_positions"
            logger.info("[US_BALANCE][SUMMARY] total_pvs_source=zero_no_positions")
    else:
        result["total_pvs"] = "0"
        result["total_pvs_source"] = "zero_no_data"
        logger.info("[US_BALANCE][SUMMARY] total_pvs_source=zero_no_data")
    
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


class USDataProvider:
    """미국주식 데이터 제공자.

    Args:
        offline: True이면 stub 데이터만 반환 (HTTP 호출 없음)
        cache_enabled: True이면 prep run 내에서 daily/price 캐시 사용
    """

    def __init__(self, offline: bool = False, cache_enabled: bool = False) -> None:
        self._offline = offline
        self._cache_enabled = cache_enabled
        self._client = None
        self._daily_cache: dict = {}
        self._price_cache: dict = {}
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
        }

    def _get_client(self):
        if self._client is None:
            from trader.us.execution.kis_us_client import KisUSClient
            self._client = KisUSClient(env="practice")
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
                }
        except Exception as exc:
            logger.warning(
                "[US_DATA][FALLBACK_STALE_FAIL] symbol=%s error=%s", symbol, exc
            )
            return None

    def get_current_price(self, symbol: str, exchange: str) -> dict:
        """현재가 조회 (KIS → DB stale fallback)."""
        cache_key = (symbol.upper(), exchange.upper())
        
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
            return result
        
        try:
            result = self._get_client().get_us_price(symbol, exchange)
            output = result.get("output", {})
            data = {
                "last": output.get("last", "0"),
                "open": output.get("open", "0"),
                "high": output.get("high", "0"),
                "low": output.get("low", "0"),
                "tvol": output.get("tvol", "0"),
                "symbol": symbol,
            }
            if self._cache_enabled:
                self._price_cache[cache_key] = data
                self.stats["price_ok_symbols"].add(symbol.upper())
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
            raise

    def get_daily_prices(self, symbol: str, exchange: str, count: int = 120) -> list[dict]:
        """일봉 데이터 조회 (xymd 기준 오름차순 정렬, KIS → DB fallback).

        전략 코드가 closes[-1]을 최신 가격으로 가정하므로 반드시 오름차순 반환.
        """
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
            result = _make_stub_daily(symbol, count)
            if self._cache_enabled:
                self._daily_cache[cache_key] = result
                self.stats["daily_ok_symbols"].add(symbol.upper())
            return result
        
        try:
            rows = self._get_client().get_us_daily_price(symbol, exchange, count)
            result = sorted(rows, key=lambda r: str(r.get("xymd", "")))
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
                    logger.info(
                        "[US_DATA][FALLBACK_DB_OK] symbol=%s count=%d", symbol, len(db_rows)
                    )
                    if self._cache_enabled:
                        self._daily_cache[cache_key] = db_rows
                        self.stats["daily_ok_symbols"].add(symbol.upper())
                    return db_rows
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

    def get_balance(self) -> dict:
        """잔고 조회."""
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
        raw = self._get_client().get_us_balance()
        return normalize_us_balance(raw)

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
        if self._offline:
            return 1000.0
        try:
            raw = self._get_client().get_us_orderable_cash(
                symbol=symbol, exchange=exchange, price=price
            )
        except Exception as exc:
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
                    return float(val)
                except (ValueError, TypeError):
                    logger.warning("[US_DATA][WARN] orderable_cash field %s not numeric: %s", key, val)
        logger.warning("[US_DATA][WARN] orderable_cash not found in response, returning 0.0")
        return 0.0
    
    def get_client_stats(self) -> dict:
        """KIS client stats 반환 (retry count 등)."""
        if self._client is None:
            return {}
        return getattr(self._client, "stats", {})
