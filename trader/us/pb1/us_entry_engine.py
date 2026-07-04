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


def _as_float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_us_trade_date(now: datetime | None = None) -> str:
    from zoneinfo import ZoneInfo
    ny = ZoneInfo("America/New_York")
    dt = (now or datetime.now(tz=ny)).astimezone(ny)
    return dt.strftime("%Y-%m-%d")


def _load_held_position_snapshot(symbol: str, provider: Any = None) -> dict:
    """Load held-position facts for add-buy decisions from DB/KIS-like snapshots."""
    sym = str(symbol or "").strip().upper()
    snapshot: dict = {}
    try:
        from trader.us.db.repos import load_us_positions_by_symbols
        rows = load_us_positions_by_symbols([sym])
        if isinstance(rows, dict):
            snapshot.update(rows.get(sym) or rows.get(symbol) or {})
    except Exception as exc:
        logger.debug("[US_ENTRY][HELD_SNAPSHOT][DB_WARN] symbol=%s error=%s", sym, exc)

    # Optional provider balance snapshot fallback for tests/live adapters.
    try:
        balance_positions = None
        for attr in ("positions", "balance_positions", "holdings"):
            value = getattr(provider, attr, None)
            if isinstance(value, list):
                balance_positions = value
                break
        if balance_positions:
            for pos in balance_positions:
                if str(pos.get("symbol") or "").strip().upper() == sym:
                    snapshot.update(pos)
                    break
        if not balance_positions:
            for method_name in ("get_balance_positions", "get_positions", "get_holdings"):
                method = getattr(provider, method_name, None)
                if callable(method):
                    rows = method()
                    if isinstance(rows, dict):
                        rows = rows.get("positions") or rows.get("holdings") or []
                    if isinstance(rows, list):
                        for pos in rows:
                            if str(pos.get("symbol") or "").strip().upper() == sym:
                                snapshot.update(pos)
                                raise StopIteration
    except StopIteration:
        pass
    except Exception as exc:
        logger.debug("[US_ENTRY][HELD_SNAPSHOT][PROVIDER_WARN] symbol=%s error=%s", sym, exc)
    return snapshot


def _held_pnl_pct(snapshot: dict, current_price: float | None = None) -> float | None:
    for key in ("unrealized_pnl_pct", "pnl_pct", "pnl_rate", "profit_rate"):
        val = _as_float_or_none(snapshot.get(key))
        if val is not None:
            return val / 100.0 if abs(val) > 1.0 else val
    avg = _as_float_or_none(snapshot.get("avg_price") or snapshot.get("avg_cost") or snapshot.get("entry_price") or snapshot.get("avg_price_usd"))
    cur = _as_float_or_none(current_price) or _as_float_or_none(snapshot.get("current_price") or snapshot.get("current_price_usd") or snapshot.get("current_px"))
    if avg and avg > 0 and cur and cur > 0:
        return (cur - avg) / avg
    return None


def _held_current_weight(snapshot: dict, capital_usd_cap: float) -> float | None:
    for key in ("current_weight", "weight", "portfolio_weight"):
        val = _as_float_or_none(snapshot.get(key))
        if val is not None:
            return val / 100.0 if val > 1.0 else val
    mv = _as_float_or_none(snapshot.get("market_value") or snapshot.get("market_value_usd") or snapshot.get("eval_amount_usd"))
    if mv is None:
        qty = _as_float_or_none(snapshot.get("qty") or snapshot.get("holding_qty"))
        price = _as_float_or_none(snapshot.get("current_price") or snapshot.get("current_price_usd") or snapshot.get("current_px"))
        if qty is not None and price is not None:
            mv = qty * price
    if mv is not None and capital_usd_cap > 0:
        return mv / capital_usd_cap
    return None


def _calc_add_position_size(*, symbol: str, price: float, available_cash_usd: float, capital_usd_cap: float, snapshot: dict) -> dict:
    target_weight = float(os.getenv("US_TARGET_POSITION_WEIGHT", "0.025") or 0.025)
    max_symbol_weight = float(os.getenv("US_MAX_POSITION_WEIGHT", "0.05") or 0.05)
    current_weight = _held_current_weight(snapshot, capital_usd_cap)
    if current_weight is None:
        return {"blocked": True, "reason": "weight_unknown", "target_weight": target_weight, "max_symbol_weight": max_symbol_weight}
    if current_weight >= max_symbol_weight:
        return {"blocked": True, "reason": "max_symbol_weight_reached", "current_weight": current_weight, "target_weight": target_weight, "max_symbol_weight": max_symbol_weight}
    if current_weight >= target_weight:
        return {"blocked": True, "reason": "target_weight_reached", "current_weight": current_weight, "target_weight": target_weight, "max_symbol_weight": max_symbol_weight}
    allowed_weight = max(0.0, min(target_weight - current_weight, max_symbol_weight - current_weight))
    if allowed_weight <= 0:
        return {"blocked": True, "reason": "full_weight", "current_weight": current_weight, "target_weight": target_weight, "max_symbol_weight": max_symbol_weight, "allowed_weight": allowed_weight}
    budget_usd = min(available_cash_usd, capital_usd_cap * allowed_weight)
    if budget_usd <= 0:
        return {"blocked": True, "reason": "insufficient_cash_for_add", "current_weight": current_weight, "target_weight": target_weight, "max_symbol_weight": max_symbol_weight, "allowed_weight": allowed_weight}
    qty = int(budget_usd // price) if price > 0 else 0
    if qty <= 0:
        return {"blocked": True, "reason": "add_qty_zero", "current_weight": current_weight, "target_weight": target_weight, "max_symbol_weight": max_symbol_weight, "allowed_weight": allowed_weight}
    notional_usd = qty * price
    projected_weight = current_weight + (notional_usd / capital_usd_cap if capital_usd_cap > 0 else 0.0)
    if projected_weight > max_symbol_weight:
        return {"blocked": True, "reason": "max_symbol_weight_reached", "current_weight": current_weight, "target_weight": target_weight, "max_symbol_weight": max_symbol_weight, "allowed_weight": allowed_weight, "projected_weight": projected_weight}
    return {
        "blocked": False,
        "qty": qty,
        "notional_usd": notional_usd,
        "current_weight": current_weight,
        "target_weight": target_weight,
        "max_symbol_weight": max_symbol_weight,
        "allowed_weight": allowed_weight,
        "projected_weight": projected_weight,
        "deployment_action": "ADD_TO_EXISTING_BUY",
    }



def _risk_clamp_new_buy_size(*, symbol: str, price: float, signal_target_notional: float, account_equity: float, current_symbol_market_value: float = 0.0, remaining_buy_budget: float = 0.0, max_position_weight: float | None = None) -> dict:
    """Clamp NEW_BUY sizing before router risk gate sees the intent."""
    max_w = float(max_position_weight if max_position_weight is not None else os.getenv("US_MAX_POSITION_WEIGHT", "0.05"))
    equity = float(account_equity or 0.0)
    allowed_notional = max(0.0, equity * max_w - float(current_symbol_market_value or 0.0))
    budget = float(remaining_buy_budget if remaining_buy_budget is not None else 0.0)
    final_notional_cap = min(float(signal_target_notional or 0.0), allowed_notional, max(0.0, budget))
    final_qty = int(final_notional_cap // price) if price > 0 else 0
    final_notional = final_qty * price
    result = {
        "allowed_notional": allowed_notional,
        "final_notional": final_notional,
        "final_qty": final_qty,
        "max_position_weight": max_w,
        "projected_weight": ((float(current_symbol_market_value or 0.0) + final_notional) / equity) if equity > 0 else 0.0,
    }
    if final_qty < 1:
        result["skip_reason"] = "INSUFFICIENT_CAPACITY_AFTER_RISK_CLAMP"
    logger.info(
        "[US_ENTRY][RISK_CLAMP] symbol=%s signal_notional=%.4f allowed_notional=%.4f "
        "remaining_buy_budget=%.4f final_qty=%d final_notional=%.4f max_position_weight=%.4f",
        symbol, float(signal_target_notional or 0.0), allowed_notional, budget, final_qty, final_notional, max_w,
    )
    return result


def _validate_new_buy_explain_contract(symbol: str, entry_meta: dict | None, entry_style: str, signal_score: float | None = None) -> tuple[bool, str]:
    data = entry_meta or {}
    style = str(entry_style or data.get("entry_style") or "").upper()
    score_keys = ("breakout_score", "pullback_score", "momentum_score")
    scores = []
    has_component_score = any(key in data for key in score_keys)
    for key in score_keys:
        try:
            scores.append(float(data.get(key) or 0.0))
        except (TypeError, ValueError):
            scores.append(0.0)
    if not has_component_score:
        try:
            scores.append(float(data.get("score") or data.get("final_score") or signal_score or 0.0))
        except (TypeError, ValueError):
            scores.append(0.0)
    if signal_score is not None:
        try:
            scores.append(float(signal_score or 0.0))
        except (TypeError, ValueError):
            pass
    if style == "SKIP" or not style:
        logger.error("[US_ENTRY][ENTRY_EXPLAIN_CONTRACT_ERROR] symbol=%s reason=entry_style_skip_for_new_buy", symbol)
        return False, "ENTRY_EXPLAIN_CONTRACT_ERROR"
    if max(scores) <= 0.0:
        logger.error("[US_ENTRY][ENTRY_EXPLAIN_CONTRACT_ERROR] symbol=%s reason=all_scores_zero", symbol)
        return False, "ENTRY_EXPLAIN_CONTRACT_ERROR"
    return True, ""

def _can_reenter_after_soft_exit(symbol: str, entry_meta: dict, now: datetime | None = None) -> bool:
    """Allow limited same-day reentry only after a recoverable soft/profit-trailing exit.

    The watchlist/prep layer can provide these fields after checking VWAP, 5m higher-low,
    and QQQ/SOXX recovery. Hard-stop exits remain blocked.
    """
    if os.getenv("US_ALLOW_REENTRY_AFTER_SOFT_EXIT", "1") not in {"1", "true", "True", "yes", "YES"}:
        return False
    last_exit_type = str(entry_meta.get("last_exit_type") or entry_meta.get("sell_exit_type") or "").lower()
    if last_exit_type in {"hard_stop", "hard_stop_loss"}:
        logger.info(
            "[US_ENTRY][REENTRY_CHECK] symbol=%s last_exit_type=%s minutes_since_sell=%s recovery_signals=%d required=%d allowed=%s",
            symbol, last_exit_type, None, 0, int(os.getenv("US_REENTRY_RECOVERY_MIN_SIGNALS", "3")), False,
        )
        return False
    if last_exit_type and last_exit_type not in {"soft_stop_loss", "profit_trailing_stop", "trailing_stop", "profit_protect"}:
        return False
    raw_minutes = entry_meta.get("minutes_since_sell")
    if raw_minutes is None:
        raw_minutes = entry_meta.get("minutes_after_sell")
    minutes_since_sell = _as_float_or_none(raw_minutes)
    recovered = [
        bool(entry_meta.get("price_above_vwap") or entry_meta.get("symbol_price_above_vwap")),
        bool(entry_meta.get("symbol_5m_low_higher") or entry_meta.get("higher_low_5m")),
        bool(entry_meta.get("qqq_recovering") or entry_meta.get("soxx_recovering")),
    ]
    required_recovery_signals = int(os.getenv("US_REENTRY_RECOVERY_MIN_SIGNALS", "3"))
    recovery_signals = sum(recovered)
    allowed_exit_type = last_exit_type in {"soft_stop_loss", "profit_trailing_stop", "trailing_stop", "profit_protect"}
    allowed = bool(
        allowed_exit_type
        and minutes_since_sell is not None
        and minutes_since_sell >= 30
        and recovery_signals >= required_recovery_signals
    )
    logger.info(
        "[US_ENTRY][REENTRY_CHECK] symbol=%s last_exit_type=%s minutes_since_sell=%s "
        "recovery_signals=%d required=%d allowed=%s",
        symbol, last_exit_type, minutes_since_sell, recovery_signals, required_recovery_signals, allowed,
    )
    return allowed


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
    *,
    allow_new_symbols: bool = True,
    allow_add_to_existing: bool = True,
    available_new_slots: int | None = None,
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
        allow_new_symbols: False이면 NOT_HELD 신규 후보는 price lookup 전에 skip
        allow_add_to_existing: HELD 기존 보유 종목 추가매수 허용 여부
        available_new_slots: 신규 심볼 잔여 슬롯 수

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
    min_new_candidates = int(os.getenv("US_ENTRY_MIN_NEW_PRICE_LOOKUP", "8"))
    max_total_lookup = int(os.getenv("US_ENTRY_MAX_TOTAL_PRICE_LOOKUP", "20"))
    lookup_limit = min(max_total_lookup, max(max_new_entries + price_lookup_buffer, min_new_candidates))
    
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
    skipped_price_lookup_due_to_full_position = 0
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
        
        entry_meta_for_reentry = entries_map.get(symbol) or {}
        # 당일 매도 차단. soft/profit-trailing exit 후 회복 컨텍스트가 명시된 경우 제한적 재진입 허용.
        if symbol in sold_today:
            if _can_reenter_after_soft_exit(symbol, entry_meta_for_reentry, now=now):
                logger.info("[US_ENTRY][REENTRY_AFTER_SOFT_EXIT] symbol=%s max_buy_ratio_of_sold_qty=0.5", symbol)
            else:
                track_skip(symbol, "sold_today")
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=sold_today", symbol)
                continue
        
        position_state = "HELD" if (current_position_symbols and symbol in current_position_symbols) else "NOT_HELD"

        # DB: 미체결 주문은 차단하되, 보유는 HELD로 분리해 추가매수 평가로 진행
        try:
            from trader.us.db.repos import has_pending_order_for_symbol_side, has_position
            if has_pending_order_for_symbol_side(symbol=symbol, side="BUY", trade_date=_resolve_us_trade_date(now)):
                position_state = "PENDING_BUY"
                track_skip(symbol, "pending_order")
                logger.info("[US_ENTRY_DECISION] symbol=%s position_state=%s action=SKIP_PENDING_BUY", symbol, position_state)
                continue
            if has_pending_order_for_symbol_side(symbol=symbol, side="SELL", trade_date=_resolve_us_trade_date(now)):
                position_state = "PENDING_SELL"
                track_skip(symbol, "pending_order")
                logger.info("[US_ENTRY_DECISION] symbol=%s position_state=%s action=SKIP_PENDING_SELL", symbol, position_state)
                continue
            if position_state == "NOT_HELD" and has_position(symbol):
                position_state = "HELD"
        except Exception as exc:
            position_state = "UNKNOWN_POSITION_STATE" if position_state == "NOT_HELD" else position_state
            logger.debug("[US_ENTRY][WARN] DB check failed symbol=%s: %s", symbol, exc)

        # Capacity guard must run before any fallback/precomputed price lookup.
        if position_state != "HELD" and not allow_new_symbols:
            skipped_price_lookup_due_to_full_position += 1
            track_skip(symbol, "max_positions_reached_new_symbol", {
                "position_count": position_count,
                "available_new_slots": available_new_slots,
                "price_lookup_skipped": True,
                "skip_stage": "before_any_price_lookup",
            })
            logger.info(
                "[US_ENTRY][SKIP] symbol=%s reason=max_positions_reached_new_symbol "
                "position_count=%s available_new_slots=%s price_lookup_skipped=1 stage=before_any_price_lookup",
                symbol, position_count, available_new_slots,
            )
            continue

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
            
            canonical_entry["position_state"] = position_state
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
            candidates.append((s, symbol, exchange, price, {"position_state": position_state}))
    
    # =========================================================================
    # Phase 2: Sort by score, then price lookup for top N
    # =========================================================================
    # Sort candidates by score (descending)
    candidates.sort(key=lambda x: x[0], reverse=True)
    
    # Price lookup plan logging: 보유/신규 후보를 분리해 top8이 보유종목으로만 채워지는 것을 방지한다.
    total_candidates = len(candidates)
    candidates_needing_price = sum(1 for c in candidates if c[3] is None)
    if has_precomputed_scores:
        add_candidates = [c for c in candidates if (c[4] or {}).get("position_state") == "HELD"]
        new_candidates = [c for c in candidates if (c[4] or {}).get("position_state") != "HELD"]
        selected_new = new_candidates[:min(min_new_candidates, lookup_limit)]
        remaining_lookup_slots = max(0, lookup_limit - len(selected_new))
        selected_add = add_candidates[:remaining_lookup_slots]
        selected_new_symbols = {c[1] for c in selected_new}
        selected_add_symbols = {c[1] for c in selected_add}
        actual_lookup_count = min(candidates_needing_price, lookup_limit)
        def lookup_priority(c):
            score = float(c[0] or 0.0)
            if c[1] in selected_new_symbols:
                return (0, -score)
            if c[1] in selected_add_symbols:
                return (1, -score)
            return (2, -score)
        candidates = sorted(
            candidates,
            key=lookup_priority,
        )
    else:
        add_candidates = []
        new_candidates = candidates
        selected_new = []
        selected_add = []
        actual_lookup_count = candidates_needing_price
    
    logger.info(
        "[US_ENTRY][PRICE_LOOKUP_PLAN] total=%d held_candidates=%d new_candidates=%d min_new=%d "
        "lookup_limit=%d selected_new=%d selected_held=%d actual_lookup=%d",
        len(symbols), len(add_candidates), len(new_candidates), min_new_candidates,
        lookup_limit, len(selected_new), len(selected_add), actual_lookup_count
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
    held_candidates_evaluated_for_add = 0

    # Price lookup 및 intent 생성 (상위 lookup_limit개만)
    for rank, (score, symbol, exchange, existing_price, entry_meta) in enumerate(candidates):
        symbol_upper_for_capacity = str(symbol or "").upper().strip()
        position_state = (entry_meta or {}).get("position_state")
        if not position_state:
            position_state = "HELD" if (current_position_symbols and symbol_upper_for_capacity in {str(s).upper().strip() for s in current_position_symbols}) else "NOT_HELD"
        if position_state == "HELD":
            held_candidates_evaluated_for_add += 1
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

        if added_count >= max_new_entries:
            track_skip(symbol, "max_new_entries_reached", {"max_new_entries": max_new_entries})
            continue

        position_state_for_order = position_state or (entry_meta or {}).get("position_state", "NOT_HELD")
        position_action = "ADD_TO_EXISTING_BUY" if position_state_for_order == "HELD" else "NEW_POSITION_BUY"
        if position_state_for_order == "HELD":
            allow_add = allow_add_to_existing and os.getenv("US_ALLOW_ADD_TO_EXISTING", "1") in {"1", "true", "TRUE", "yes", "YES"}
            allow_avg_down = os.getenv("US_ALLOW_AVERAGING_DOWN", "0") in {"1", "true", "TRUE", "yes", "YES"}
            min_add_pnl = float(os.getenv("US_ADD_MIN_PNL_PCT", "0.0") or 0.0)
            if not allow_add:
                track_skip(symbol, "add_to_existing_disabled")
                logger.info("[US_ENTRY_DECISION] symbol=%s position_state=HELD action=SKIP_ADD_DISABLED", symbol)
                continue
            held_snapshot = _load_held_position_snapshot(symbol, provider=provider)
            pnl_pct = _held_pnl_pct(held_snapshot, current_price=price)
            if pnl_pct is None:
                track_skip(symbol, "add_pnl_unknown")
                logger.info("[US_ENTRY_DECISION] symbol=%s position_state=HELD action=SKIP_ADD_PNL_UNKNOWN", symbol)
                continue
            if pnl_pct < 0 and not allow_avg_down:
                track_skip(symbol, "no_averaging_down")
                logger.info("[US_ENTRY_DECISION] symbol=%s position_state=HELD action=SKIP_NO_AVERAGING_DOWN", symbol)
                continue
            if pnl_pct < min_add_pnl:
                track_skip(symbol, "add_min_pnl_not_met")
                logger.info("[US_ENTRY_DECISION] symbol=%s position_state=HELD action=SKIP_ADD_MIN_PNL pnl_pct=%.4f", symbol, pnl_pct)
                continue
            max_add_count = int(os.getenv("US_MAX_ADD_COUNT_PER_SYMBOL", "2") or 2)
            add_count = int(
                held_snapshot.get("add_count_today")
                or held_snapshot.get("add_count")
                or held_snapshot.get("pyramid_add_count")
                or 0
            )
            if add_count >= max_add_count:
                track_skip(symbol, "max_add_count_reached")
                logger.info(
                    "[US_ENTRY_DECISION] symbol=%s position_state=HELD action=SKIP_MAX_ADD_COUNT add_count=%d max_add_count=%d",
                    symbol, add_count, max_add_count,
                )
                continue
            sizing = _calc_add_position_size(
                symbol=symbol,
                price=price,
                available_cash_usd=available_cash_usd,
                capital_usd_cap=capital_usd_cap,
                snapshot=held_snapshot,
            )
            if sizing["blocked"]:
                reason = sizing.get("reason", "add_sizing_blocked")
                if reason in {"full_weight", "weight_unknown", "max_symbol_weight_reached"}:
                    action = "SKIP_FULL_WEIGHT" if reason in {"full_weight", "max_symbol_weight_reached"} else "SKIP_ADD_WEIGHT_UNKNOWN"
                    track_skip(symbol, reason)
                    logger.info("[US_ENTRY_DECISION] symbol=%s position_state=HELD action=%s current_weight=%s", symbol, action, sizing.get("current_weight"))
                    continue
                track_skip(symbol, reason)
                logger.info("[US_ENTRY_DECISION] symbol=%s position_state=HELD action=SKIP_ADD_SIZING reason=%s", symbol, reason)
                continue
            qty = sizing["qty"]
            notional = sizing["notional_usd"]
            qty_held = _as_float_or_none(held_snapshot.get("qty") or held_snapshot.get("holding_qty") or held_snapshot.get("quantity")) or 0.0
            held_snapshot_market_value = (
                _as_float_or_none(held_snapshot.get("market_value_usd"))
                or _as_float_or_none(held_snapshot.get("market_value"))
                or _as_float_or_none(held_snapshot.get("eval_amount_usd"))
                or (qty_held * price)
            )
            logger.info("[US_ENTRY_DECISION] symbol=%s position_state=HELD action=ADD_BUY reason=pyramid_allowed position_action=%s pnl_pct=%.4f current_weight=%.4f projected_weight=%.4f", symbol, position_action, pnl_pct, sizing.get("current_weight", 0.0), sizing.get("projected_weight", 0.0))
        else:
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

            risk_clamp = _risk_clamp_new_buy_size(
                symbol=symbol,
                price=price,
                signal_target_notional=float(sizing["notional_usd"]),
                account_equity=capital_usd_cap,
                current_symbol_market_value=0.0,
                remaining_buy_budget=available_cash_usd,
            )
            if risk_clamp.get("final_qty", 0) < 1:
                track_skip(symbol, "INSUFFICIENT_CAPACITY_AFTER_RISK_CLAMP", risk_clamp)
                logger.info("[US_ENTRY][SKIP] symbol=%s reason=INSUFFICIENT_CAPACITY_AFTER_RISK_CLAMP allowed_notional=%.4f", symbol, risk_clamp.get("allowed_notional", 0.0))
                continue
            qty = int(risk_clamp["final_qty"])
            notional = float(risk_clamp["final_notional"])
            sizing["projected_weight"] = risk_clamp.get("projected_weight", 0.0)
            sizing["max_symbol_weight"] = risk_clamp.get("max_position_weight")
            sizing["allowed_notional"] = risk_clamp.get("allowed_notional")
            logger.info("[US_ENTRY_DECISION] symbol=%s position_state=NOT_HELD action=NEW_BUY position_action=%s", symbol, position_action)

        limit_price = round(price * (1 + float(os.getenv("US_LIMIT_PRICE_BAND_PCT", "0.005"))), 4)

        import hashlib
        trade_date_for_key = _resolve_us_trade_date(now)
        today_str = trade_date_for_key.replace("-", "")
        
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

        if position_action == "NEW_POSITION_BUY":
            entry_style_for_contract = str((entry_meta or {}).get("entry_style") or _resolve_entry_signal_type(entry_meta) or "momentum")
            ok_contract, contract_reason = _validate_new_buy_explain_contract(symbol, entry_meta, entry_style_for_contract, signal_score=score)
            if not ok_contract:
                track_skip(symbol, contract_reason, {"entry_style": entry_style_for_contract})
                continue

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
            "position_state": position_state_for_order,
            "position_action": position_action,
            "qty": qty,
            "limit_price": limit_price,
            "notional_usd": notional,
            **({
                "current_position_market_value_usd": held_snapshot_market_value,
                "current_weight": sizing.get("current_weight"),
                "target_weight": sizing.get("target_weight"),
                "max_symbol_weight": sizing.get("max_symbol_weight"),
                "allowed_weight": sizing.get("allowed_weight"),
                "projected_weight": sizing.get("projected_weight"),
                "deployment_action": sizing.get("deployment_action", "ADD_TO_EXISTING_BUY"),
            } if position_state_for_order == "HELD" else {}),
            "score": score,
            "rank": rank + 1,
            "client_order_key": client_order_key,
            "strategy": "us_pb1",
            "entry_style": str((entry_meta or {}).get("entry_style") or _resolve_entry_signal_type(entry_meta) or "momentum").lower(),
            "projected_weight": sizing.get("projected_weight"),
            "max_symbol_weight": sizing.get("max_symbol_weight"),
            "allowed_notional": sizing.get("allowed_notional"),
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
            "trade_date": trade_date_for_key,
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
                "position_state": position_state_for_order,
                "position_action": position_action,
                **({
                    "capital_deployment": {
                        "current_position_market_value_usd": held_snapshot_market_value,
                        "current_weight": sizing.get("current_weight"),
                        "target_weight": sizing.get("target_weight"),
                        "max_symbol_weight": sizing.get("max_symbol_weight"),
                        "allowed_weight": sizing.get("allowed_weight"),
                        "projected_weight": sizing.get("projected_weight"),
                        "deployment_action": sizing.get("deployment_action", "ADD_TO_EXISTING_BUY"),
                    }
                } if position_state_for_order == "HELD" else {}),
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
    skipped_max_positions_reached = _sr("max_positions_reached") + _sr("max_positions_reached_new_symbol")
    skipped_insufficient_cash = _sr("insufficient_cash")
    skipped_new_symbol_full_position = _sr("max_positions_reached_new_symbol")

    logger.info(
        "[US_ENTRY][SKIP_SUMMARY] "
        "watchlist_total=%d evaluated_count=%d selected_count=%d entry_intents=%d "
        "skipped_has_kis_position=%d skipped_has_position=%d skipped_pending_order=%d "
        "skipped_sold_today=%d skipped_score_missing=%d skipped_score_below_min=%d "
        "skipped_price_unavailable=%d skipped_price_nonpositive=%d skipped_sizing_blocked=%d "
        "skipped_qty_zero=%d skipped_max_positions_reached=%d skipped_insufficient_cash=%d "
        "skipped_capacity_precheck=%d skipped_new_symbol_full_position=%d "
        "skipped_price_lookup_due_to_full_position=%d held_candidates_evaluated_for_add=%d",
        total_processed, scored_count, added_count, len(intents),
        skipped_has_kis_position, skipped_has_position, skipped_pending_order,
        skipped_sold_today, skipped_score_missing, skipped_score_below_min,
        skipped_price_unavailable, skipped_price_nonpositive, skipped_sizing_blocked,
        skipped_qty_zero, skipped_max_positions_reached, skipped_insufficient_cash,
        int(not allow_new_symbols), skipped_new_symbol_full_position,
        skipped_price_lookup_due_to_full_position, held_candidates_evaluated_for_add,
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
