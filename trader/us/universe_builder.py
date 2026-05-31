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
    """최근 14일 ATR% 계산 (normalize_daily_rows 정규화 이후 데이터 대응)."""
    if len(daily_rows) < 15:
        return None
    try:
        trs = []
        rows = sorted(daily_rows, key=lambda r: str(r.get("xymd", "") or r.get("date", "")))
        recent = rows[-15:]
        for i in range(1, len(recent)):
            # normalize_daily_rows 이후 close/high/low 필드 우선, fallback clos
            prev_close = recent[i - 1].get("close") or float(str(recent[i - 1].get("clos", 0) or 0).replace(",", ""))
            high = recent[i].get("high") or float(str(recent[i].get("high", 0) or 0).replace(",", ""))
            low = recent[i].get("low") or float(str(recent[i].get("low", 0) or 0).replace(",", ""))
            c = recent[i].get("close") or float(str(recent[i].get("clos", 0) or 0).replace(",", ""))
            if not isinstance(prev_close, (int, float)):
                prev_close = float(prev_close or 0)
            if not isinstance(high, (int, float)):
                high = float(high or 0)
            if not isinstance(low, (int, float)):
                low = float(low or 0)
            if not isinstance(c, (int, float)):
                c = float(c or 0)
            if prev_close <= 0:
                continue
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)
        if not trs:
            return None
        atr = sum(trs) / len(trs)
        # normalize_daily_rows 이후 close 필드 우선
        last_close_val = rows[-1].get("close")
        if last_close_val is None:
            last_close_val = float(str(rows[-1].get("clos", 0) or 0).replace(",", ""))
        last_close = float(last_close_val or 0)
        if last_close <= 0:
            return None
        return round(atr / last_close, 4)
    except Exception:
        return None


def _extract_latest_close(daily_rows: list[dict]) -> float | None:
    """일봉 rows의 마지막 close 값을 반환한다."""
    if not daily_rows:
        return None
    rows = sorted(daily_rows, key=lambda r: str(r.get("xymd", "") or r.get("date", "")))
    last = rows[-1]
    close_val = last.get("close")
    if close_val is None:
        close_val = last.get("clos")
    if close_val is None:
        return None
    try:
        val = float(str(close_val).replace(",", ""))
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


def _compute_avg_volume(daily_rows: list[dict], days: int = 20) -> float:
    """최근 N일 평균 거래량 (normalize_daily_rows 정규화 대응)."""
    if not daily_rows:
        return 0.0
    try:
        rows = sorted(daily_rows, key=lambda r: str(r.get("xymd", "") or r.get("date", "")))
        recent = rows[-days:]
        volumes = []
        for r in recent:
            vol = r.get("volume")
            if vol is None:
                vol = r.get("tvol", 0)
            try:
                v = float(str(vol or 0).replace(",", ""))
                if v > 0:
                    volumes.append(v)
            except (ValueError, TypeError):
                pass
        return sum(volumes) / len(volumes) if volumes else 0.0
    except Exception:
        return 0.0


def _compute_avg_dollar_volume(daily_rows: list[dict], days: int = 20) -> float:
    """최근 N일 평균 거래대금 (normalize_daily_rows 정규화 대응)."""
    if not daily_rows:
        return 0.0
    try:
        rows = sorted(daily_rows, key=lambda r: str(r.get("xymd", "") or r.get("date", "")))
        recent = rows[-days:]
        dollar_vols = []
        for r in recent:
            vol = r.get("volume")
            if vol is None:
                vol = r.get("tvol", 0)
            close = r.get("close")
            if close is None:
                close = r.get("clos", 0)
            try:
                v = float(str(vol or 0).replace(",", ""))
                c = float(str(close or 0).replace(",", ""))
                if v > 0 and c > 0:
                    dollar_vols.append(v * c)
            except (ValueError, TypeError):
                pass
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
    # 수정 3: 기본값 120 → 60, relaxed=30
    strict_history_days = _env_int("US_MIN_HISTORY_DAYS", 60)
    relaxed_history_days = max(30, strict_history_days // 2)
    max_atr_pct = _env_float("US_MAX_ATR_PCT", 0.18)
    universe_min = _env_int("US_DYNAMIC_UNIVERSE_MIN", 80)
    universe_target = _env_int("US_DYNAMIC_UNIVERSE_TARGET", 300)

    # 수정 5: manual seed / core ETF - price만 있으면 fallback 허용
    _CORE_SEED_SYMBOLS: frozenset[str] = frozenset({
        "SPY", "QQQ", "QQQM", "SMH", "SOXX",
        "NVDA", "MSFT", "AAPL", "AMZN", "META", "GOOGL", "AVGO",
    })

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

    # ── 데이터 조회 + 3단계 필터 ────────────────────────────────────────────
    filter_counts = {
        "passed": 0,
        "failed_price": 0,
        "failed_volume": 0,
        "failed_dollar_volume": 0,
        "failed_history": 0,
        "failed_atr": 0,
    }

    # 3단계 버킷
    strict_pass: list[dict] = []
    relaxed_pass: list[dict] = []
    fallback_pass: list[dict] = []

    # 실패 상세 추적 (수정 6)
    _fail_details: list[dict] = []

    warnings: list[str] = []
    errors: list[str] = []

    for symbol in all_tickers:
        exchange = _resolve_exchange(symbol)
        current_price: float | None = None
        latest_close: float | None = None
        daily: list[dict] = []
        history_days: int = 0
        current_price_raw_val: str = "None"

        # --- current price 조회 ---
        try:
            current = provider.get_current_price(symbol, exchange)
            _raw_last = current.get("last", 0) or current.get("close", 0) or 0
            _p = float(str(_raw_last).replace(",", ""))
            if _p > 0:
                current_price = _p
            current_price_raw_val = str(_raw_last)
        except Exception as exc:
            logger.debug(
                "[US_UNIVERSE_BUILDER][SKIP] symbol=%s reason=price_fetch_error error=%s",
                symbol, exc,
            )

        # --- daily price 조회 ---
        try:
            daily = provider.get_daily_prices(symbol, exchange, as_of_date=as_of_date)
            history_days = len(daily)
            latest_close = _extract_latest_close(daily)
        except Exception as exc:
            logger.debug(
                "[US_UNIVERSE_BUILDER][SKIP] symbol=%s reason=daily_fetch_error error=%s",
                symbol, exc,
            )

        # --- price fallback (수정 2) ---
        price: float | None = current_price
        price_source = "current"
        if (price is None or price <= 0) and latest_close and latest_close > 0:
            price = latest_close
            price_source = "daily_close"
            logger.info(
                "[US_UNIVERSE_BUILDER][PRICE_FALLBACK] symbol=%s source=daily_close price=%.4f",
                symbol, price,
            )

        price_ok = price is not None and price > 0 and price >= min_price

        if price_ok:
            logger.debug(
                "[US_UNIVERSE_BUILDER][PRICE_OK] symbol=%s price=%.4f source=%s",
                symbol, price, price_source,
            )

        # --- 각 실패 항목 상세 로그 (수정 6) ---
        close_nonnull = sum(
            1 for r in daily
            if (r.get("close") is not None and r.get("close", 0) > 0)
            or (r.get("clos") is not None and str(r.get("clos", "0")) not in ("0", "0.0", ""))
        )
        volume_nonnull = sum(
            1 for r in daily
            if (r.get("volume") is not None and r.get("volume", 0) > 0)
            or (r.get("tvol") is not None and str(r.get("tvol", "0")) not in ("0", "0.0", ""))
        )

        avg_vol = _compute_avg_volume(daily, 20)
        avg_dv = _compute_avg_dollar_volume(daily, 20)
        atr_pct = _compute_atr_pct(daily)

        # strict 조건 평가
        strict_ok = (
            price_ok
            and history_days >= strict_history_days
            and avg_vol >= min_avg_volume
            and avg_dv >= min_avg_dollar_volume
            and (atr_pct is None or atr_pct <= max_atr_pct)
        )

        # relaxed 조건 평가
        relaxed_ok = (
            price_ok
            and history_days >= relaxed_history_days
            and avg_vol > 0
            and avg_dv > 0
        )

        # fallback 조건 평가 (수정 5: core seed는 price만 있으면 허용)
        is_core_seed = symbol in _CORE_SEED_SYMBOLS or "manual_seed" in ticker_tags.get(symbol, [])
        fallback_ok = price_ok and (latest_close is not None) and is_core_seed

        asset_type = "etf" if _is_etf(symbol) else "stock"
        base_entry = {
            "symbol": symbol,
            "exchange": exchange,
            "asset_type": asset_type,
            "source_tags": list(ticker_tags.get(symbol, [])),
            "price": round(price, 4) if price else 0.0,
            "avg_volume_20d": round(avg_vol, 0),
            "avg_dollar_volume_20d": round(avg_dv, 0),
            "history_days": history_days,
            "atr_pct": atr_pct if atr_pct is not None else 0.0,
        }

        if strict_ok:
            strict_pass.append({**base_entry, "filter_mode": "strict"})
        elif relaxed_ok:
            relaxed_pass.append({**base_entry, "filter_mode": "relaxed"})
        elif fallback_ok:
            fallback_pass.append({
                **base_entry,
                "filter_mode": "fallback_seed_price_only",
                "warning": "insufficient_history_but_seed_allowed",
            })
        else:
            # 실패 사유 추적
            fail_reason: str
            if not price_ok:
                fail_reason = "failed_price"
                filter_counts["failed_price"] += 1
            elif history_days < relaxed_history_days:
                fail_reason = "failed_history"
                filter_counts["failed_history"] += 1
            elif avg_vol < min_avg_volume:
                fail_reason = "failed_volume"
                filter_counts["failed_volume"] += 1
            elif avg_dv < min_avg_dollar_volume:
                fail_reason = "failed_dollar_volume"
                filter_counts["failed_dollar_volume"] += 1
            elif atr_pct is not None and atr_pct > max_atr_pct:
                fail_reason = "failed_atr"
                filter_counts["failed_atr"] += 1
            else:
                fail_reason = "failed_unknown"

            _fail_details.append({
                "symbol": symbol,
                "price": price,
                "latest_close": latest_close,
                "current_price": current_price,
                "daily_rows": history_days,
                "history_days": history_days,
                "close_nonnull": close_nonnull,
                "volume_nonnull": volume_nonnull,
                "reason": fail_reason,
            })

    # ── strict/relaxed/fallback 집계 로그 ─────────────────────────────────
    logger.info("[US_UNIVERSE_BUILDER][FILTER][STRICT] passed=%d", len(strict_pass))
    logger.info("[US_UNIVERSE_BUILDER][FILTER][RELAXED] passed=%d", len(relaxed_pass))
    logger.info("[US_UNIVERSE_BUILDER][FILTER][FALLBACK] passed=%d", len(fallback_pass))

    # ── 최종 selected 구성 (strict 우선) ─────────────────────────────────
    # strict → relaxed → fallback 순으로 누적
    filtered_symbols: list[dict] = list(strict_pass)
    selected_set: set[str] = {s["symbol"] for s in filtered_symbols}

    for entry in relaxed_pass:
        if entry["symbol"] not in selected_set:
            filtered_symbols.append(entry)
            selected_set.add(entry["symbol"])

    for entry in fallback_pass:
        if entry["symbol"] not in selected_set:
            filtered_symbols.append(entry)
            selected_set.add(entry["symbol"])

    filtered_count = len(filtered_symbols)
    filter_counts["passed"] = filtered_count

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

    # ── 실패 상세 로그 (수정 6) ─────────────────────────────────────────────
    if _fail_details:
        price_fail_samples = [d["symbol"] for d in _fail_details if d["reason"] == "failed_price"][:10]
        if price_fail_samples:
            logger.warning(
                "[US_UNIVERSE_BUILDER][FILTER_FAIL_SAMPLE] reason=failed_price symbols=%s",
                ",".join(price_fail_samples),
            )
        for det in _fail_details[:20]:  # 최대 20개 상세 로그
            logger.warning(
                "[US_UNIVERSE_BUILDER][FILTER_FAIL_DETAIL] symbol=%s price=%s latest_close=%s"
                " current_price=%s daily_rows=%d history_days=%d"
                " close_nonnull=%d volume_nonnull=%d reason=%s",
                det["symbol"],
                det["price"],
                det["latest_close"],
                det["current_price"],
                det["daily_rows"],
                det["history_days"],
                det["close_nonnull"],
                det["volume_nonnull"],
                det["reason"],
            )

    # ── 상태 결정 (수정 7) ────────────────────────────────────────────────
    if filtered_count < 30:
        status = "ERROR"
        errors.append(f"filtered_count={filtered_count} < hard_min=30")
        logger.error("[US_UNIVERSE_BUILDER][ERROR] filtered_count=%d < 30 → hard fail", filtered_count)
    elif filtered_count < 80:
        status = "OK_WITH_WARNINGS"
        warnings.append(f"filtered_count={filtered_count} < 80 (used_relaxed_or_fallback)")
    elif filtered_count < universe_min:
        status = "OK_WITH_WARNINGS"
        warnings.append(f"filtered_count={filtered_count} < universe_min={universe_min}")
    elif filtered_count < universe_target:
        status = "OK_WITH_WARNINGS"
        warnings.append(f"filtered_count={filtered_count} < universe_target={universe_target}")
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
