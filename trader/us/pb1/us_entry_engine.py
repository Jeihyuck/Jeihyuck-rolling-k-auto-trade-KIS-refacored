# -*- coding: utf-8 -*-
"""US PB1 Entry Engine.

한국장 PB1 진입 전략을 미국장용으로 이식.

진입 조건:
1. MA20/MA50 위 추세
2. breakout / momentum / pullback entry_style
3. RS / VCP / momentum / pullback score
4. 거래량 조건
5. rank 기반 우선순위
6. 당일 매도 후 재매수 차단
7. 미체결 주문 존재 시 추가 주문 차단
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from trader.us.data_provider import USDataProvider  # test patch surface

# US Explanation System
from trader.us.pb1.us_explain import (
    build_us_entry_explanation,
    log_us_entry_decision,
    validate_explanations_batch,
)

logger = logging.getLogger(__name__)


def normalize_us_entry_input(
    tickers: list[str] | list[dict] | None,
    watchlist_entries: list[dict] | None,
) -> tuple[list[str], list[dict]]:
    """Entry 입력을 정규화.
    
    Args:
        tickers: symbol list 또는 dict list
        watchlist_entries: locked watchlist rows (dict list)
        
    Returns:
        (symbols: list[str], entries: list[dict])
        - symbols: 평가 대상 symbol list
        - entries: watchlist metadata rows (empty if not provided)
    """
    symbols: list[str] = []
    entries: list[dict] = []
    
    # watchlist_entries 우선 사용
    if watchlist_entries:
        for row in watchlist_entries:
            sym = row.get("symbol")
            if sym and isinstance(sym, str):
                symbols.append(sym)
                entries.append(row)
            else:
                logger.warning(
                    "[US_ENTRY][INPUT_NORMALIZE] invalid row, symbol=%s type=%s",
                    sym, type(sym).__name__
                )
        logger.info(
            "[US_ENTRY][INPUT_NORMALIZE] source=locked_watchlist rows=%d symbols=%d",
            len(watchlist_entries), len(symbols)
        )
        return symbols, entries
    
    # tickers가 없으면 empty 반환
    if not tickers:
        return [], []
    
    # tickers가 list[str]이면 그대로 사용
    if tickers and isinstance(tickers[0], str):
        symbols = list(tickers)
        logger.info(
            "[US_ENTRY][INPUT_NORMALIZE] source=ticker_list symbols=%d", len(symbols)
        )
        return symbols, []
    
    # tickers가 list[dict]이면 symbol/exchange 추출
    for item in tickers:
        if isinstance(item, dict):
            sym = item.get("symbol")
            if sym and isinstance(sym, str):
                symbols.append(sym)
                # exchange 정보도 포함하여 entries에 추가
                entries.append({
                    "symbol": sym,
                    "exchange": item.get("exchange", ""),
                    "score": item.get("score"),
                    "rank": item.get("rank"),
                    "meta": item.get("meta", {}),
                })
            else:
                logger.warning(
                    "[US_ENTRY][INPUT_NORMALIZE] dict but invalid symbol: %s", item
                )
        elif isinstance(item, str):
            symbols.append(item)
        else:
            logger.warning(
                "[US_ENTRY][INPUT_NORMALIZE] unexpected type: %s", type(item).__name__
            )
    
    logger.info(
        "[US_ENTRY][INPUT_NORMALIZE] source=mixed_input symbols=%d entries=%d",
        len(symbols), len(entries)
    )
    return symbols, entries


def _to_float_list(rows: list[dict], key: str = "clos") -> list[float]:
    out = []
    for r in rows:
        try:
            out.append(float(r.get(key, 0) or 0))
        except (ValueError, TypeError):
            pass
    return out


def _ma(prices: list[float], n: int) -> float | None:
    if len(prices) < n:
        return None
    return sum(prices[-n:]) / n


def _momentum_pct(prices: list[float], n: int) -> float | None:
    if len(prices) < n + 1:
        return None
    old = prices[-(n + 1)]
    if old <= 0:
        return None
    return (prices[-1] - old) / old


def score_symbol(symbol: str, daily_prices: list[dict], current_price: dict) -> float | None:
    """종목 점수 계산.

    Returns:
        0.0~1.0 점수, 또는 None (진입 불가)
    """
    closes = _to_float_list(daily_prices, "clos")
    volumes = _to_float_list(daily_prices, "tvol")

    if len(closes) < 55:
        return None

    last = closes[-1]
    if last <= 0:
        return None

    ma20 = _ma(closes, 20)
    ma50 = _ma(closes, 50)

    if ma20 is None or ma50 is None:
        return None

    # ── 추세 조건 ─────────────────────────────────────────────────────────────
    if not (last > ma20 > ma50 * 0.98):
        return None

    # ── 모멘텀 ────────────────────────────────────────────────────────────────
    mom20 = _momentum_pct(closes, 20)
    mom50 = _momentum_pct(closes, 50)

    if mom20 is None or mom50 is None:
        return None

    # 음의 모멘텀이면 스킵
    if mom20 < 0 or mom50 < 0:
        return None

    # ── 눌림목 (pullback) ─────────────────────────────────────────────────────
    recent_high = max(closes[-20:])
    if recent_high <= 0:
        return None
    pullback = (recent_high - last) / recent_high  # 0 = 고점, 0.15 = 15% 조정

    # 너무 눌리면 스킵 (15% 초과)
    if pullback > 0.15:
        return None

    # ── 거래량 조건 ───────────────────────────────────────────────────────────
    if len(volumes) >= 20:
        avg_vol = sum(volumes[-20:]) / 20
        today_vol = volumes[-1]
        vol_ratio = today_vol / avg_vol if avg_vol > 0 else 1.0
    else:
        vol_ratio = 1.0

    # 거래량 지나치게 부족하면 스킵
    if vol_ratio < 0.3:
        return None

    # ── 최종 점수 계산 ────────────────────────────────────────────────────────
    # 모멘텀 기여 (40%)
    mom_score = min(1.0, (mom20 + mom50) / 0.30)  # 합산 30% 이상이면 만점

    # 눌림목 기여 (30%): 얕은 눌림목일수록 좋음
    pullback_score = 1.0 - (pullback / 0.15) if pullback > 0 else 1.0

    # 거래량 기여 (30%)
    vol_score = min(1.0, vol_ratio / 2.0)

    total = 0.4 * mom_score + 0.3 * pullback_score + 0.3 * vol_score
    return round(max(0.0, min(1.0, total)), 3)


def _resolve_entry_signal_type(entry_meta: dict | None) -> str:
    """entry_meta에서 entry_signal_type을 추론한다.

    중요: entry_signal_type (pullback/breakout/momentum)은
    book/horizon (SWING_BOOK/DAY_BOOK)과 완전히 다른 개념이다.
    pullback으로 진입해도 SWING_BOOK으로 관리할 수 있다.
    """
    if not entry_meta:
        return "unknown"
    # 명시적 field 우선
    sig = entry_meta.get("entry_signal_type") or entry_meta.get("signal_type")
    if sig:
        return str(sig).lower()
    # entry_style_selected 또는 entry_style로 추론
    style = str(
        entry_meta.get("entry_style_selected")
        or entry_meta.get("entry_style")
        or entry_meta.get("style")
        or ""
    ).lower()
    if "pullback" in style:
        return "pullback"
    if "breakout" in style:
        return "breakout"
    if "momentum" in style:
        return "momentum"
    if "vcp" in style:
        return "vcp"
    return "unknown"


def generate_entry_intents(
    tickers: list[str] | list[dict] | None,
    provider: Any,
    sold_today: set[str],
    available_cash_usd: float,
    position_count: int,
    capital_usd_cap: float,
    now: datetime | None = None,
    max_new_entries: int | None = None,
    watchlist_entries: list[dict] | None = None,
    current_position_symbols: set[str] | None = None,
) -> list[dict]:
    """진입 intent 목록 생성.

    Args:
        tickers: 평가 대상 ticker list (list[str] 또는 list[dict])
        provider: USDataProvider 인스턴스
        sold_today: 당일 매도 완료 종목 집합 (재매수 차단)
        available_cash_usd: 실제 주문 가능 잔고
        position_count: 현재 보유 포지션 수
        capital_usd_cap: 미국장 예산 USD cap
        now: 현재 시각 (None이면 실시간)
        max_new_entries: tick당 최대 신규 진입 수
        watchlist_entries: locked watchlist rows (authoritative input)
        current_position_symbols: KIS balance에서 얻은 현재 보유 종목 집합

    Returns:
        list of order intent dict
    """
    from trader.us.symbols import resolve_exchange
    from trader.us.pb1.us_position_sizing import calc_position_size

    if max_new_entries is None:
        max_new_entries = int(os.getenv("US_MAX_NEW_ENTRIES_PER_TICK", "3"))

    # 입력 정규화
    symbols, entries = normalize_us_entry_input(tickers, watchlist_entries)
    
    # 입력 contract 검증
    if not symbols:
        logger.warning("[US_ENTRY][INPUT_CONTRACT] no symbols to evaluate")
        return []
    
    logger.info(
        "[US_ENTRY][INPUT_CONTRACT] symbols=%d entries=%d schema_ok=1",
        len(symbols), len(entries)
    )
    
    # entries를 symbol → entry dict로 변환 (빠른 조회용)
    entries_map = {e["symbol"]: e for e in entries} if entries else {}
    
    # Price lookup 최적화 설정
    price_lookup_buffer = int(os.getenv("US_ENTRY_PRICE_LOOKUP_BUFFER", "5"))
    lookup_limit = max_new_entries + price_lookup_buffer
    
    # Precomputed score가 있는지 확인 (watchlist가 locked되어 있는 경우)
    has_precomputed_scores = bool(entries_map)

    # Skip reason tracking for observability
    skip_reasons: dict[str, int] = {}
    skip_details: list[dict] = []  # For diagnostics artifact
    buy_explanations: list[dict] = []  # BUY decision explanations
    skip_explanations: list[dict] = []  # SKIP decision explanations
    
    def track_skip(symbol: str, reason: str, details: dict | None = None):
        """Track skip reason for summary and diagnostics."""
        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
        skip_detail = {"symbol": symbol, "reason": reason}
        if details:
            skip_detail.update(details)
        skip_details.append(skip_detail)
        
        # Build skip explanation for observability
        entry_data = entries_map.get(symbol, {})
        if entry_data:
            skip_explanation = build_us_entry_explanation(
                symbol=symbol,
                entry_data=entry_data,
                decision="SKIP",
                skip_reason=reason,
            )
            skip_explanations.append(skip_explanation)

    # =========================================================================
    # Phase 1: Filter + Score Validation (NO price lookup yet if precomputed)
    # =========================================================================
    candidates: list[tuple[float, str, str, float | None, dict | None]] = []  # (score, symbol, exchange, price, entry_meta)
    seen_symbols: set[str] = set()  # 중복 symbol 차단용
    held_skipped = 0  # 보유종목으로 스킵된 수

    for symbol in symbols:
        # symbol이 str인지 확인
        if not isinstance(symbol, str):
            logger.error(
                "[US_ENTRY][CONTRACT_FAIL] symbol must be str, got %s", type(symbol).__name__
            )
            raise TypeError(f"[US_ENTRY][INPUT_CONTRACT_FAIL] symbol must be str, got {type(symbol).__name__}")
        
        # 동일 tick 내 중복 symbol 차단
        if symbol in seen_symbols:
            track_skip(symbol, "duplicate_in_tick")
            logger.debug(
                "[US_ENTRY][SKIP] symbol=%s reason=duplicate_in_tick",
                symbol
            )
            continue
        seen_symbols.add(symbol)
        
        # 당일 매도 차단
        if symbol in sold_today:
            track_skip(symbol, "sold_today")
            logger.info("[US_ENTRY][SKIP] symbol=%s reason=sold_today", symbol)
            continue
        
        # KIS balance 기반 보유종목 차단 (우선순위 높음)
        if current_position_symbols and symbol in current_position_symbols:
            held_skipped += 1
            track_skip(symbol, "has_kis_position")
            logger.info("[US_ENTRY][SKIP] symbol=%s reason=has_kis_position", symbol)
            continue

        # DB: 미체결 주문 차단
        try:
            from trader.us.db.repos import has_pending_order, has_position
            if has_pending_order(symbol):
                track_skip(symbol, "pending_order")
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=pending_order", symbol)
                continue
            # DB: 이미 보유 중이면 차단
            if has_position(symbol):
                held_skipped += 1
                track_skip(symbol, "has_position")
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=has_position", symbol)
                continue
        except Exception as exc:
            logger.debug("[US_ENTRY][WARN] DB check failed symbol=%s: %s", symbol, exc)

        # entries_map에 있으면 precomputed score 사용
        entry_meta = entries_map.get(symbol)
        if entry_meta:
            # Canonicalization import
            from trader.us.score_columns import extract_us_score, canonicalize_us_watchlist_row
            
            # Canonicalize the entry row
            try:
                canonical_entry = canonicalize_us_watchlist_row(entry_meta)
            except Exception as exc:
                logger.warning(
                    "[US_ENTRY][CANONICALIZE_FAIL] symbol=%s: %s",
                    symbol, exc
                )
                canonical_entry = entry_meta
            
            # Extract score with alias recovery
            s, score_source = extract_us_score(canonical_entry, "final", return_source=True)
            
            # Log precomputed score BEFORE validation
            logger.debug(
                "[US_ENTRY][PRECOMPUTED_SCORE] symbol=%s score=%.6f source=%s",
                symbol, s if s is not None else 0.0, score_source
            )
            
            # Score validation
            if s is None:
                raw_score = entry_meta.get("score")
                track_skip(symbol, "score_missing_after_alias_resolution", {
                    "raw_score": raw_score,
                    "score_source": score_source,
                })
                logger.info(
                    "[US_ENTRY][SKIP] symbol=%s reason=score_missing_after_alias_resolution "
                    "canonical_score=None score_source=%s raw_score=%s",
                    symbol, score_source, raw_score
                )
                continue
            
            if s <= 0:
                raw_score = entry_meta.get("score")
                track_skip(symbol, "score_zero_after_alias_resolution", {
                    "canonical_score": s,
                    "raw_score": raw_score,
                    "score_source": score_source,
                })
                logger.info(
                    "[US_ENTRY][SKIP] symbol=%s reason=score_zero_after_alias_resolution "
                    "canonical_score=%.6f score_source=%s raw_score=%s",
                    symbol, s, score_source, raw_score
                )
                continue
            
            # Minimum entry score validation
            min_entry_score = float(os.getenv("US_MIN_ENTRY_SCORE", "0.05"))
            if s < min_entry_score:
                track_skip(symbol, "score_below_min", {
                    "score": s,
                    "min_entry_score": min_entry_score,
                })
                logger.info(
                    "[US_ENTRY][SKIP] symbol=%s reason=score_below_min score=%.6f min=%.6f",
                    symbol, s, min_entry_score
                )
                continue
            
            exchange = canonical_entry.get("exchange", "")
            if not exchange:
                exchange = resolve_exchange(symbol)
            
            # Phase 1: NO price lookup yet (if precomputed score exists)
            candidates.append((s, symbol, exchange, None, canonical_entry))  # price=None, entry_meta=canonical_entry
            
        else:
            # precomputed score가 없으면 실시간 계산 필요 (기존 로직 유지)
            try:
                exchange = resolve_exchange(symbol)
                daily = provider.get_daily_prices(symbol, exchange, count=120)
                current = provider.get_current_price(symbol, exchange)
            except Exception as exc:
                track_skip(symbol, "daily_price_unavailable", {"error": str(exc)})
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=daily_price_unavailable error=%s", symbol, exc)
                continue

            try:
                s = score_symbol(symbol, daily, current)
            except Exception as exc:
                track_skip(symbol, "score_calculation_error", {"error": str(exc)})
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=score_calculation_error error=%s", symbol, exc)
                continue

            if s is None:
                track_skip(symbol, "score_calculation_failed")
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=score_calculation_failed", symbol)
                continue
            
            if s <= 0:
                track_skip(symbol, "score_zero_or_negative", {"score": s})
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=score_zero_or_negative score=%.6f", symbol, s)
                continue

            try:
                price = float(current.get("last", 0))
            except (ValueError, TypeError):
                track_skip(symbol, "current_price_unavailable")
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=current_price_unavailable", symbol)
                continue

            if price <= 0:
                track_skip(symbol, "current_price_invalid", {"price": price})
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=current_price_invalid price=%.2f", symbol, price)
                continue
            
            # 실시간 계산된 경우 price를 이미 갖고 있으므로 candidates에 추가
            # entry_meta는 없으므로 None
            candidates.append((s, symbol, exchange, price, None))
    
    # =========================================================================
    # Phase 2: Sort by score, then price lookup for top N
    # =========================================================================
    # Sort candidates by score (descending)
    candidates.sort(key=lambda x: x[0], reverse=True)
    
    # Price lookup plan logging
    total_candidates = len(candidates)
    candidates_needing_price = sum(1 for c in candidates if c[3] is None)
    actual_lookup_count = min(candidates_needing_price, lookup_limit) if has_precomputed_scores else candidates_needing_price
    
    logger.info(
        "[US_ENTRY][PRICE_LOOKUP_PLAN] total=%d held_skipped=%d lookup_limit=%d actual_lookup=%d",
        len(symbols), held_skipped, lookup_limit, actual_lookup_count
    )
    
    # Entry engine 내부 dedupe 요약
    input_symbols_count = len(symbols)
    unique_symbols_count = len(seen_symbols)
    skipped_duplicates = input_symbols_count - unique_symbols_count
    logger.info(
        "[US_ENTRY][DEDUP] input_rows=%d unique_symbols=%d skipped_duplicates=%d candidates=%d",
        input_symbols_count, unique_symbols_count, skipped_duplicates, len(candidates)
    )

    intents: list[dict] = []
    added_count = 0
    price_lookup_count = 0  # 실제 price lookup 횟수 추적
    
    # Price lookup 및 intent 생성 (상위 lookup_limit개만)
    for rank, (score, symbol, exchange, existing_price, entry_meta) in enumerate(candidates):
        # 이미 max_new_entries 만큼 추가했으면 종료
        if added_count >= max_new_entries:
            break

        # Price lookup (필요한 경우)
        if existing_price is None:
            # Optimization: 상위 lookup_limit개만 price lookup (precomputed score가 있는 경우)
            if has_precomputed_scores and price_lookup_count >= lookup_limit:
                logger.debug(
                    "[US_ENTRY][SKIP] symbol=%s rank=%d reason=beyond_lookup_limit limit=%d",
                    symbol, rank + 1, lookup_limit
                )
                track_skip(symbol, "beyond_lookup_limit", {"rank": rank + 1, "limit": lookup_limit})
                continue  # 다음 candidate로 (break 아님 - 이미 price 있는 것은 처리)

            price_lookup_count += 1
            try:
                current = provider.get_current_price(symbol, exchange)
            except Exception as exc:
                track_skip(symbol, "current_price_unavailable", {"error": str(exc)})
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=current_price_unavailable error=%s", symbol, exc)
                continue

            try:
                price = float(current.get("last", 0))
            except (ValueError, TypeError):
                track_skip(symbol, "current_price_unavailable")
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=current_price_unavailable", symbol)
                continue

            if price <= 0:
                track_skip(symbol, "current_price_invalid", {"price": price})
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=current_price_invalid price=%.2f", symbol, price)
                continue
        else:
            # 이미 price가 있음 (runtime calculation)
            price = existing_price

        sizing = calc_position_size(
            price=price,
            available_cash_usd=available_cash_usd,
            capital_usd_cap=capital_usd_cap,
            position_count=position_count + added_count,
            score=score,
        )

        if sizing["blocked"]:
            track_skip(symbol, "sizing_blocked", {"sizing_reason": sizing["reason"]})
            logger.info(
                "[US_ENTRY][SKIP] symbol=%s reason=sizing_blocked sizing_reason=%s",
                symbol, sizing["reason"],
            )
            continue

        qty = sizing["qty"]
        notional = sizing["notional_usd"]

        limit_price = round(price * (1 + float(os.getenv("US_LIMIT_PRICE_BAND_PCT", "0.005"))), 4)

        import hashlib
        from datetime import date
        
        # force_now가 있으면 그 날짜 사용
        if now:
            today_str = now.date().strftime("%Y%m%d")
            trade_date_for_key = now.date().strftime("%Y-%m-%d")
        else:
            today_str = date.today().strftime("%Y%m%d")
            trade_date_for_key = date.today().strftime("%Y-%m-%d")
        
        key_raw = f"{symbol}_{today_str}_BUY"
        client_order_key = hashlib.sha256(key_raw.encode()).hexdigest()[:24]

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # Duplicate pre-entry skip: DB에 이미 존재하는 client_order_key 차단
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            from trader.us.db.repos import load_today_order_keys
            existing_keys = load_today_order_keys(trade_date=trade_date_for_key)
            if client_order_key in existing_keys:
                track_skip(symbol, "existing_order_key", {"key": client_order_key})
                logger.info(
                    "[US_ENTRY][SKIP] symbol=%s reason=existing_order_key key=%s trade_date=%s",
                    symbol, client_order_key, trade_date_for_key
                )
                continue
        except Exception as exc:
            logger.warning(
                "[US_ENTRY][DUPLICATE_CHECK_WARN] symbol=%s: %s",
                symbol, exc
            )

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # Final order cap enforcement: 최종 방어 로직
        # US_MAX_ORDER_USD를 초과하는 intent는 절대 append하지 않는다.
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        order_cap_usd = float(os.getenv("US_MAX_ORDER_USD", "2500"))
        
        if notional > order_cap_usd:
            # qty를 줄여서 order_cap 이하로 맞춤
            old_qty = qty
            old_notional = notional
            new_qty = int(order_cap_usd // price)
            
            if new_qty <= 0:
                # 가격이 너무 높아서 1주도 못 사는 경우 skip
                track_skip(symbol, "order_cap_qty_zero", {
                    "price": price,
                    "order_cap_usd": order_cap_usd,
                    "old_qty": old_qty,
                    "old_notional": old_notional,
                })
                logger.warning(
                    "[US_ENTRY][SKIP] symbol=%s reason=order_cap_qty_zero price=%.2f cap=%.2f old_qty=%d",
                    symbol, price, order_cap_usd, old_qty,
                )
                continue
            
            # qty를 축소하고 notional 재계산
            qty = new_qty
            notional = qty * price
            
            # 축소 후에도 여전히 cap 초과인지 재검증 (안전망)
            if notional > order_cap_usd:
                track_skip(symbol, "intent_notional_exceeds_order_cap_after_sizing", {
                    "price": price,
                    "order_cap_usd": order_cap_usd,
                    "new_qty": qty,
                    "new_notional": notional,
                })
                logger.warning(
                    "[US_ENTRY][SKIP] symbol=%s reason=intent_notional_exceeds_order_cap_after_sizing "
                    "new_notional=%.2f cap=%.2f",
                    symbol, notional, order_cap_usd,
                )
                continue
            
            logger.warning(
                "[US_ENTRY][RESIZE_TO_ORDER_CAP] symbol=%s old_qty=%d new_qty=%d old_notional=%.2f new_notional=%.2f cap=%.2f",
                symbol, old_qty, qty, old_notional, notional, order_cap_usd,
            )

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # Build entry explanation
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        entry_explanation = None
        if entry_meta:
            entry_explanation = build_us_entry_explanation(
                symbol=symbol,
                entry_data=entry_meta,
                decision="BUY",
                skip_reason=None,
            )
            buy_explanations.append(entry_explanation)
            
            # Log WHY_BUY
            log_us_entry_decision(symbol, "BUY", entry_explanation)

        intent = {
            "symbol": symbol,
            "exchange": exchange,
            "side": "BUY",
            "qty": qty,
            "limit_price": limit_price,
            "notional_usd": notional,
            "score": score,
            "rank": rank + 1,
            "client_order_key": client_order_key,
            "strategy": "us_pb1",
            "entry_style": "momentum",
            # ── entry_meta: book / horizon / exit_policy ─────────────────────
            # 중요: entry_signal_type (pullback/breakout/momentum)과
            #        book/horizon/exit_policy (포지션 관리 방식)은 다르다.
            # PB1 기본값은 SWING_BOOK / SWING_CARRY 이다.
            "book": os.getenv("US_DEFAULT_ENTRY_BOOK", "SWING_BOOK"),
            "horizon": os.getenv("US_DEFAULT_ENTRY_HORIZON", "SWING_CARRY"),
            "exit_policy": os.getenv("US_DEFAULT_EXIT_POLICY", "US_SWING_DEFAULT"),
            "entry_strategy": "us_pb1",
            "entry_signal_type": _resolve_entry_signal_type(entry_meta),
            "entry_session": now.strftime("%p").lower().replace("am", "am").replace("pm", "afternoon") if now else "am",
            "trade_date": trade_date if 'trade_date' in locals() else (now.strftime("%Y-%m-%d") if now else ""),
            "partial_exit_allowed": os.getenv("US_SELL_PARTIAL_ALLOWED", "0") == "1",
            "source": "locked_watchlist",
            "min_hold_minutes": int(os.getenv("US_SWING_MIN_HOLD_MINUTES", "390")),
            "meta": {
                "book": os.getenv("US_DEFAULT_ENTRY_BOOK", "SWING_BOOK"),
                "horizon": os.getenv("US_DEFAULT_ENTRY_HORIZON", "SWING_CARRY"),
                "exit_policy": os.getenv("US_DEFAULT_EXIT_POLICY", "US_SWING_DEFAULT"),
                "entry_strategy": "us_pb1",
                "entry_signal_type": _resolve_entry_signal_type(entry_meta),
                "partial_exit_allowed": os.getenv("US_SELL_PARTIAL_ALLOWED", "0") == "1",
                "source": "locked_watchlist",
                "schema_version": 1,
            },
        }
        logger.info(
            "[US_ENTRY][META] symbol=%s book=%s horizon=%s exit_policy=%s "
            "entry_strategy=us_pb1 entry_signal_type=%s min_hold_minutes=%s partial_exit_allowed=%d",
            symbol,
            intent["book"], intent["horizon"], intent["exit_policy"],
            intent["entry_signal_type"],
            intent["min_hold_minutes"],
            int(intent["partial_exit_allowed"]),
        )
        
        # Add explanation fields if available
        if entry_explanation:
            intent["entry_style_selected"] = entry_explanation.get("entry_style_selected", "ENTRY_GENERIC")
            intent["entry_component"] = entry_explanation.get("entry_component", "generic")
            intent["score_breakdown"] = entry_explanation.get("score_breakdown", {})
            intent["reasons"] = entry_explanation.get("reasons", [])
            intent["filters_passed"] = entry_explanation.get("filters_passed", [])
            intent["explanation_quality"] = entry_explanation.get("explanation_quality", "MINIMAL")
        
        intents.append(intent)
        added_count += 1

        logger.info(
            "[US_ENTRY][INTENT] symbol=%s rank=%d score=%.6f qty=%d notional=%.2f",
            symbol, rank + 1, score, qty, notional,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Skip Summary for Observability (표준 필드명)
    # ─────────────────────────────────────────────────────────────────────────
    total_processed = len(symbols)
    scored_count = len(candidates)  # candidates = successfully scored symbols

    # skip_reasons key → 표준 필드명 매핑
    def _sr(*keys: str) -> int:
        return sum(skip_reasons.get(k, 0) for k in keys)

    skipped_has_kis_position = _sr("has_kis_position")
    skipped_has_position = _sr("has_position")
    skipped_pending_order = _sr("pending_order")
    skipped_sold_today = _sr("sold_today")
    skipped_score_missing = _sr(
        "score_missing_after_alias_resolution",
        "score_calculation_failed",
        "score_calculation_error",
    )
    skipped_score_below_min = _sr(
        "score_below_min",
        "score_zero_after_alias_resolution",
        "score_zero_or_negative",
    )
    skipped_price_unavailable = _sr(
        "current_price_unavailable",
        "daily_price_unavailable",
    )
    skipped_price_nonpositive = _sr("current_price_invalid")
    skipped_sizing_blocked = _sr("sizing_blocked")
    skipped_qty_zero = _sr(
        "order_cap_qty_zero",
        "intent_notional_exceeds_order_cap_after_sizing",
    )
    skipped_max_positions_reached = _sr("max_positions_reached")
    skipped_insufficient_cash = _sr("insufficient_cash")

    logger.info(
        "[US_ENTRY][SKIP_SUMMARY] "
        "watchlist_total=%d evaluated_count=%d selected_count=%d entry_intents=%d "
        "skipped_has_kis_position=%d skipped_has_position=%d skipped_pending_order=%d "
        "skipped_sold_today=%d skipped_score_missing=%d skipped_score_below_min=%d "
        "skipped_price_unavailable=%d skipped_price_nonpositive=%d skipped_sizing_blocked=%d "
        "skipped_qty_zero=%d skipped_max_positions_reached=%d skipped_insufficient_cash=%d",
        total_processed, scored_count, added_count, len(intents),
        skipped_has_kis_position, skipped_has_position, skipped_pending_order,
        skipped_sold_today, skipped_score_missing, skipped_score_below_min,
        skipped_price_unavailable, skipped_price_nonpositive, skipped_sizing_blocked,
        skipped_qty_zero, skipped_max_positions_reached, skipped_insufficient_cash,
    )
    logger.info("[US_ENTRY][INTENTS] count=%d", len(intents))
    
    # Write score diagnostics artifact if any score issues detected
    score_issue_reasons = {
        "score_missing_after_alias_resolution",
        "score_zero_after_alias_resolution",
        "score_below_min",
        "score_calculation_failed",
        "score_calculation_error",
        "score_zero_or_negative",
    }
    
    score_issue_details = [d for d in skip_details if d["reason"] in score_issue_reasons]
    
    if score_issue_details:
        try:
            import json
            import csv
            
            os.makedirs("repo/artifacts", exist_ok=True)
            
            # JSON artifact
            json_path = "repo/artifacts/us_score_diagnostics.json"
            with open(json_path, "w") as f:
                json.dump(score_issue_details, f, indent=2, default=str)
            
            # CSV artifact
            csv_path = "repo/artifacts/us_score_diagnostics.csv"
            if score_issue_details:
                keys = set()
                for d in score_issue_details:
                    keys.update(d.keys())
                fieldnames = sorted(keys)
                
                with open(csv_path, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(score_issue_details)
            
            logger.info(
                "[US_SCORE_DIAG][SAVED] rows=%d json=%s csv=%s",
                len(score_issue_details), json_path, csv_path
            )
        except Exception as exc:
            logger.warning("[US_SCORE_DIAG][SAVE_FAILED] %s", exc)

    # ─────────────────────────────────────────────────────────────────────────
    # Explanation Quality Validation
    # ─────────────────────────────────────────────────────────────────────────
    all_explanations = buy_explanations + skip_explanations
    if all_explanations:
        quality_report = validate_explanations_batch(all_explanations)
        logger.info(
            "[US_ENTRY][EXPLANATION_QUALITY] total=%d full=%d partial=%d minimal=%d missing=%d summary=%s warning=%s",
            quality_report["total_count"],
            quality_report["full_count"],
            quality_report["partial_count"],
            quality_report["minimal_count"],
            quality_report["missing_count"],
            quality_report["quality_summary"],
            quality_report["quality_warning"],
        )
        
        if quality_report["quality_warning"]:
            logger.warning(
                "[US_ENTRY][EXPLANATION_QUALITY_WARNING] %s",
                quality_report["quality_summary"],
            )
    else:
        logger.warning("[US_ENTRY][EXPLANATION_QUALITY] no explanations generated")

    logger.info("[US_ENTRY][EVAL][DONE] entry_intents=%d", len(intents))
    return intents
