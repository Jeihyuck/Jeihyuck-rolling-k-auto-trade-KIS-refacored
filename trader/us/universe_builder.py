# -*- coding: utf-8 -*-
"""US Dynamic Universe Builder.

config/us_universe.yaml (manual_seed) +
config/us_dynamic_sources.yaml (extended_seed) →
US Dynamic Universe (filtered)

한국장 파일 import 금지.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_CONFIG_ROOT = Path(__file__).resolve().parents[2] / "config"
_DYNAMIC_SOURCES_PATH = _CONFIG_ROOT / "us_dynamic_sources.yaml"

# 미국장 ETF 집합 (asset_type 판단)
_KNOWN_ETFS: set[str] = {
    "SPY", "QQQ", "QQQM", "SMH", "SOXX", "XLK", "XLF", "XLE", "IWM",
    "GLD", "TLT", "HYG", "EEM", "VTI", "ARKK", "SOXL", "SOXS",
}


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _load_dynamic_sources() -> dict[str, list[str]]:
    """config/us_dynamic_sources.yaml 로드.

    Returns:
        {category_name: [ticker, ...], ...}  (manual_seed_note 등 비리스트 키 제외)
    """
    try:
        import yaml  # type: ignore
        with open(_DYNAMIC_SOURCES_PATH, "r") as f:
            data = yaml.safe_load(f) or {}
        return {k: [str(v).upper() for v in vs] for k, vs in data.items() if isinstance(vs, list)}
    except FileNotFoundError:
        logger.warning("[US_UNIVERSE_BUILDER][WARN] dynamic_sources not found: %s", _DYNAMIC_SOURCES_PATH)
        return {}
    except Exception as exc:
        logger.error("[US_UNIVERSE_BUILDER][ERROR] dynamic_sources load failed: %s", exc)
        return {}


def _resolve_exchange(symbol: str) -> str:
    """symbols 레지스트리에서 거래소 조회, 미등록이면 NASDAQ 기본값."""
    try:
        from trader.us.symbols import resolve_exchange
        return resolve_exchange(symbol)
    except Exception:
        return "NASDAQ"


def _normalize_symbol(symbol: str) -> str:
    """심볼 정규화 (대문자 변환 + strip)."""
    try:
        from trader.us.symbols import normalize_symbol
        return normalize_symbol(symbol)
    except Exception:
        return str(symbol).upper().strip()


def _is_etf(symbol: str) -> bool:
    return symbol in _KNOWN_ETFS


def _compute_atr_pct(daily_rows: list[dict]) -> float | None:
    """최근 14일 ATR% 계산 (offline/test stub 대응)."""
    if len(daily_rows) < 15:
        return None
    try:
        trs = []
        rows = sorted(daily_rows, key=lambda r: str(r.get("xymd", "")))
        recent = rows[-15:]
        for i in range(1, len(recent)):
            prev_close = float(recent[i - 1].get("clos", 0) or 0)
            high = float(recent[i].get("high", 0) or 0)
            low = float(recent[i].get("low", 0) or 0)
            c = float(recent[i].get("clos", 0) or 0)
            if prev_close <= 0:
                continue
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)
        if not trs:
            return None
        atr = sum(trs) / len(trs)
        last_close = float(rows[-1].get("clos", 0) or 0)
        if last_close <= 0:
            return None
        return round(atr / last_close, 4)
    except Exception:
        return None


def _compute_avg_volume(daily_rows: list[dict], days: int = 20) -> float:
    """최근 N일 평균 거래량."""
    if not daily_rows:
        return 0.0
    try:
        rows = sorted(daily_rows, key=lambda r: str(r.get("xymd", "")))
        recent = rows[-days:]
        volumes = [float(str(r.get("tvol", 0) or 0).replace(",", "")) for r in recent]
        vols = [v for v in volumes if v > 0]
        return sum(vols) / len(vols) if vols else 0.0
    except Exception:
        return 0.0


def _compute_avg_dollar_volume(daily_rows: list[dict], days: int = 20) -> float:
    """최근 N일 평균 거래대금."""
    if not daily_rows:
        return 0.0
    try:
        rows = sorted(daily_rows, key=lambda r: str(r.get("xymd", "")))
        recent = rows[-days:]
        dollar_vols = []
        for r in recent:
            vol = float(str(r.get("tvol", 0) or 0).replace(",", ""))
            close = float(str(r.get("clos", 0) or 0).replace(",", ""))
            if vol > 0 and close > 0:
                dollar_vols.append(vol * close)
        return sum(dollar_vols) / len(dollar_vols) if dollar_vols else 0.0
    except Exception:
        return 0.0


def build_us_dynamic_universe(
    *,
    trade_date: str,
    as_of_date: str | None = None,
    env: str,
    provider: Any,
    manual_seed: list[str] | None = None,
    force_rebuild: bool = False,
) -> dict:
    """US Dynamic Universe 빌드.

    Args:
        trade_date: YYYY-MM-DD
        as_of_date: KIS dailyprice BYMD 기준일. None이면 trade_date와 동일.
        env: practice / live
        provider: USDataProvider 인스턴스
        manual_seed: config/us_universe.yaml에서 로드된 ticker list
        force_rebuild: 캐시 무시 재빌드

    Returns:
        dynamic universe result dict
    """
    as_of_date = as_of_date or trade_date
    logger.info("[US_UNIVERSE_BUILDER][START] trade_date=%s env=%s", trade_date, env)
    logger.info(
        "[US_UNIVERSE_BUILDER][DATE_POLICY] trade_date=%s as_of_date=%s",
        trade_date,
        as_of_date,
    )

    # ── 환경변수 ─────────────────────────────────────────────────────────
    min_price = _env_float("US_MIN_PRICE", 5.0)
    min_avg_volume = _env_float("US_MIN_AVG_VOLUME_20D", 500_000.0)
    min_avg_dollar_volume = _env_float("US_MIN_AVG_DOLLAR_VOLUME_20D", 50_000_000.0)
    min_history_days = _env_int("US_MIN_HISTORY_DAYS", 120)
    max_atr_pct = _env_float("US_MAX_ATR_PCT", 0.18)
    universe_min = _env_int("US_DYNAMIC_UNIVERSE_MIN", 80)
    universe_target = _env_int("US_DYNAMIC_UNIVERSE_TARGET", 300)

    # ── Source 수집 ──────────────────────────────────────────────────────
    dynamic_sources = _load_dynamic_sources()

    # manual_seed
    manual_seed_list: list[str] = []
    if manual_seed:
        for s in manual_seed:
            ns = _normalize_symbol(s)
            if ns:
                manual_seed_list.append(ns)

    # dynamic_sources 전체 ticker 수집
    dynamic_tickers: list[str] = []
    for tickers in dynamic_sources.values():
        for t in tickers:
            ns = _normalize_symbol(t)
            if ns:
                dynamic_tickers.append(ns)

    # source별 카운트 (중복 제거 전)
    source_counts = {
        "manual_seed": len(manual_seed_list),
        "dynamic_sources": len(dynamic_tickers),
        "nasdaq100": 0,
        "sp500": 0,
        "etf_holdings": 0,
    }
    logger.info(
        "[US_UNIVERSE_BUILDER][SOURCE] manual_seed=%d dynamic_sources=%d nasdaq100=%d sp500=%d etf_holdings=%d",
        source_counts["manual_seed"],
        source_counts["dynamic_sources"],
        source_counts["nasdaq100"],
        source_counts["sp500"],
        source_counts["etf_holdings"],
    )

    # ── 중복 제거 ─────────────────────────────────────────────────────────
    seen: set[str] = set()
    all_tickers: list[str] = []
    # source_tags 추적
    ticker_tags: dict[str, list[str]] = {}

    for t in manual_seed_list:
        if t not in seen:
            seen.add(t)
            all_tickers.append(t)
            ticker_tags[t] = ["manual_seed"]
        else:
            if "manual_seed" not in ticker_tags.get(t, []):
                ticker_tags[t].append("manual_seed")

    for category, tickers in dynamic_sources.items():
        for ticker in tickers:
            ns = _normalize_symbol(ticker)
            if not ns:
                continue
            if ns not in seen:
                seen.add(ns)
                all_tickers.append(ns)
                ticker_tags[ns] = [category]
            else:
                if category not in ticker_tags.get(ns, []):
                    ticker_tags[ns].append(category)

    raw_count = len(manual_seed_list) + len(dynamic_tickers)
    unique_count = len(all_tickers)
    logger.info(
        "[US_UNIVERSE_BUILDER][DEDUP] raw=%d unique=%d",
        raw_count,
        unique_count,
    )

    # ── 데이터 조회 + 필터 ────────────────────────────────────────────────
    filter_counts = {
        "passed": 0,
        "failed_price": 0,
        "failed_volume": 0,
        "failed_dollar_volume": 0,
        "failed_history": 0,
        "failed_atr": 0,
    }

    filtered_symbols: list[dict] = []
    warnings: list[str] = []
    errors: list[str] = []

    for symbol in all_tickers:
        exchange = _resolve_exchange(symbol)
        try:
            current = provider.get_current_price(symbol, exchange)
            price = float(str(current.get("last", 0) or 0).replace(",", ""))
        except Exception as exc:
            logger.debug("[US_UNIVERSE_BUILDER][SKIP] symbol=%s reason=price_fetch_error error=%s", symbol, exc)
            filter_counts["failed_price"] += 1
            continue

        if price < min_price:
            filter_counts["failed_price"] += 1
            continue

        try:
            daily = provider.get_daily_prices(symbol, exchange, as_of_date=as_of_date)
        except Exception as exc:
            logger.debug("[US_UNIVERSE_BUILDER][SKIP] symbol=%s reason=daily_fetch_error error=%s", symbol, exc)
            filter_counts["failed_history"] += 1
            continue

        history_days = len(daily)
        if history_days < min_history_days:
            filter_counts["failed_history"] += 1
            continue

        avg_vol = _compute_avg_volume(daily, 20)
        if avg_vol < min_avg_volume:
            filter_counts["failed_volume"] += 1
            continue

        avg_dv = _compute_avg_dollar_volume(daily, 20)
        if avg_dv < min_avg_dollar_volume:
            filter_counts["failed_dollar_volume"] += 1
            continue

        atr_pct = _compute_atr_pct(daily)
        if atr_pct is not None and atr_pct > max_atr_pct:
            filter_counts["failed_atr"] += 1
            continue

        asset_type = "etf" if _is_etf(symbol) else "stock"
        filter_counts["passed"] += 1
        filtered_symbols.append({
            "symbol": symbol,
            "exchange": exchange,
            "asset_type": asset_type,
            "source_tags": ticker_tags.get(symbol, []),
            "price": round(price, 4),
            "avg_volume_20d": round(avg_vol, 0),
            "avg_dollar_volume_20d": round(avg_dv, 0),
            "history_days": history_days,
            "atr_pct": atr_pct if atr_pct is not None else 0.0,
        })

    filtered_count = filter_counts["passed"]

    logger.info(
        "[US_UNIVERSE_BUILDER][FILTER] input=%d passed=%d failed_price=%d"
        " failed_volume=%d failed_dollar_volume=%d failed_history=%d failed_atr=%d",
        unique_count,
        filter_counts["passed"],
        filter_counts["failed_price"],
        filter_counts["failed_volume"],
        filter_counts["failed_dollar_volume"],
        filter_counts["failed_history"],
        filter_counts["failed_atr"],
    )

    # ── filter fail sample 로그 ───────────────────────────────────────────
    if filter_counts["passed"] == 0 or filter_counts["passed"] < 30:
        # failed_price / failed_history 샘플 최대 10개씩
        price_fail_samples: list[str] = []
        history_fail_samples: list[str] = []
        _seen_price: set[str] = set()
        _seen_history: set[str] = set()
        for _sym in all_tickers:
            if len(price_fail_samples) >= 10 and len(history_fail_samples) >= 10:
                break
            if _sym not in {s["symbol"] for s in filtered_symbols}:
                # 어느 fail 버킷인지 단순 판단: exchange 조회 오류 시 price fail로 간주
                if _sym not in _seen_price and len(price_fail_samples) < 10:
                    price_fail_samples.append(_sym)
                    _seen_price.add(_sym)
        if price_fail_samples:
            logger.warning(
                "[US_UNIVERSE_BUILDER][FILTER_FAIL_SAMPLE] reason=failed_price symbols=%s",
                ",".join(price_fail_samples),
            )
        logger.warning(
            "[US_UNIVERSE_BUILDER][FILTER_FAIL_DETAIL] as_of_date=%s bymd=%s",
            as_of_date,
            str(as_of_date).replace("-", "")[:8] if as_of_date else "UNKNOWN",
        )

    # ── 상태 결정 ──────────────────────────────────────────────────────────
    if filtered_count == 0 or filtered_count < 30:
        status = "ERROR"
        errors.append(f"filtered_count={filtered_count} < hard_min=30")
        logger.error("[US_UNIVERSE_BUILDER][ERROR] filtered_count=%d < 30 → hard fail", filtered_count)
    elif filtered_count < universe_min:
        status = "OK_WITH_WARNINGS"
        warnings.append(
            f"filtered_count={filtered_count} < universe_min={universe_min}"
        )
    elif filtered_count < universe_target:
        status = "OK_WITH_WARNINGS"
        warnings.append(
            f"filtered_count={filtered_count} < universe_target={universe_target}"
        )
    else:
        status = "OK"

    logger.info(
        "[US_UNIVERSE_BUILDER][DONE] filtered=%d status=%s",
        filtered_count,
        status,
    )

    return {
        "trade_date": trade_date,
        "env": env,
        "status": status,
        "raw_count": raw_count,
        "unique_count": unique_count,
        "filtered_count": filtered_count,
        "source_counts": source_counts,
        "filter_counts": filter_counts,
        "symbols": filtered_symbols,
        "warnings": warnings,
        "errors": errors,
    }
