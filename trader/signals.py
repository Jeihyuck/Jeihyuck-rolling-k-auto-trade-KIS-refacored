"""시그널 계산 및 시세/밸런스 조회 보조 함수."""
from __future__ import annotations

import logging
from datetime import datetime
import time
from typing import Any, Dict, List, Optional, Tuple

from .core_constants import (
    ALLOW_WHEN_CLOSED,
    BAD_ENTRY_MAX_BELOW_VWAP_RATIO,
    BAD_ENTRY_MAX_MA20_DIST,
    BAD_ENTRY_MAX_PULLBACK,
    CHAMPION_A_RULES,
    CHAMPION_MAX_MDD,
    CHAMPION_MIN_SHARPE,
    CHAMPION_MIN_TRADES,
    CHAMPION_MIN_WINRATE,
    GOOD_ENTRY_MA20_RANGE,
    GOOD_ENTRY_MAX_FROM_PEAK,
    GOOD_ENTRY_MIN_INTRADAY_SIG,
    GOOD_ENTRY_MIN_RR,
    GOOD_ENTRY_PULLBACK_RANGE,
    KST,
    MOMENTUM_OVERRIDES_FORCE_SELL,
    MOM_FAST,
    MOM_SLOW,
    MOM_TH_PCT,
    PULLBACK_DAYS,
    PULLBACK_LOOKBACK,
    PULLBACK_REVERSAL_BUFFER_PCT,
    SLIPPAGE_ENTER_GUARD_PCT,
    USE_PULLBACK_ENTRY,
    VWAP_TOL,
    logger,
    DAILY_CAPITAL,
    W_MIN_ONE,
    W_MAX_ONE,
)
from .core_utils import _get_daily_candles_cached, _to_float, _to_int, _with_retry
from .kis_wrapper import KisAPI, NetTemporaryError, DataEmptyError, DataShortError
from .metrics import vwap_guard

__all__ = [
    "_safe_get_price",
    "_fetch_balances",
    "_get_effective_ord_cash",
    "_get_daily_candles_cached",
    "_detect_pullback_reversal",
    "_classify_champion_grade",
    "_compute_daily_entry_context",
    "_compute_intraday_entry_context",
    "is_bad_entry",
    "is_good_entry",
    "_get_intraday_1min",
    "_compute_vwap_from_1min",
    "_compute_intraday_momentum",
    "is_strong_momentum_vwap",
    "get_20d_return_pct",
    "is_strong_momentum",
    "_percentile_rank",
    "_has_bullish_trend_structure",
    "_weight_to_qty",
    "_notional_to_qty",
    "_get_atr",
]

# === [ANCHOR: PRICE_CACHE] 현재가 캐시 & 서킷브레이커 ===
_LAST_PRICE_CACHE: Dict[str, Dict[str, Any]] = {}  # code -> {"px": float, "ts": epoch}
_PRICE_CB: Dict[str, Dict[str, float]] = {}          # code -> {"fail": int, "until": epoch}

# === [ANCHOR: BALANCE_CACHE] 잔고 캐싱 (루프 15초 단일 호출) ===
_BALANCE_CACHE: Dict[str, Any] = {"ts": 0.0, "balances": []}

def _safe_get_price(
    kis: KisAPI,
    code: str,
    ttl_sec: int = 5,
    stale_ok_sec: int = 30,
    *,
    with_source: bool = False,
) -> Optional[float | Tuple[float, str]]:
    import time as _t
    now = _t.time()

    def _store_and_return(val: float, source: str, log_level: Optional[int] = None):
        _LAST_PRICE_CACHE[code] = {"px": float(val), "ts": now, "source": source}
        if log_level:
            logger.log(log_level, f"[PRICE_SRC] {code} ← {source} ({float(val):.2f})")
        return (float(val), source) if with_source else float(val)

    # 0) 서킷브레이커: 최근 실패 누적이면 잠시 건너뛴다
    cb = _PRICE_CB.get(code, {"fail": 0, "until": 0})
    primary_allowed = now >= cb.get("until", 0)

    # 장마감이면 캐시/종가로 대체
    try:
        if not kis.is_market_open() and not ALLOW_WHEN_CLOSED:
            ent = _LAST_PRICE_CACHE.get(code)
            if ent:
                src = ent.get("source") or "cache_close"
                return (float(ent["px"]), src) if with_source else float(ent["px"])
            if hasattr(kis, "get_close_price"):
                try:
                    close_px = kis.get_close_price(code)
                    if close_px and float(close_px) > 0:
                        val = float(close_px)
                        return _store_and_return(val, "close_after")
                except Exception:
                    pass
            return None
    except Exception:
        pass

    # 1) 캐시 최신이면 반환
    ent = _LAST_PRICE_CACHE.get(code)
    if ent and (now - ent["ts"] <= ttl_sec):
        src = ent.get("source") or "cache_recent"
        return (float(ent["px"]), src) if with_source else float(ent["px"])

    # 2) 1차 소스
    if primary_allowed:
        try:
            px = _with_retry(kis.get_current_price, code)
            if px is not None and float(px) > 0:
                val = float(px)
                _PRICE_CB[code] = {"fail": 0, "until": 0}
                return _store_and_return(val, "realtime")
            else:
                logger.warning(f"[PRICE_GUARD] {code} 현재가 무효값({px})")
        except Exception as e:
            fail = int(cb.get("fail", 0)) + 1
            cool = min(60, 3 * fail)
            _PRICE_CB[code] = {"fail": fail, "until": now + cool}
            logger.error(f"[NET/API 장애] {code} 현재가 1차조회 실패({e}) → cool {cool}s")

    # 3) 보조 소스
    try:
        if hasattr(kis, "get_quote_snapshot"):
            q = kis.get_quote_snapshot(code)
            cand = None
            if isinstance(q, dict):
                for k in ("tp", "trade_price", "prpr", "close", "price"):
                    v = q.get(k)
                    if v and float(v) > 0:
                        cand = float(v); break
            if cand and cand > 0:
                return _store_and_return(cand, "snapshot")

        if hasattr(kis, "get_best_ask") and hasattr(kis, "get_best_bid"):
            ask = kis.get_best_ask(code)
            bid = kis.get_best_bid(code)
            if ask and bid and float(ask) > 0 and float(bid) > 0:
                mid = (float(ask) + float(bid)) / 2.0
                return _store_and_return(mid, "mid_quote")
    except Exception as e:
        logger.warning(f"[PRICE_FALLBACK_FAIL] {code} 보조소스 실패: {e}")

    def _historical_fallback() -> Optional[Tuple[Optional[float], Optional[str]]]:
        """
        실시간 조회 실패 시 가격 대체값 계산.

        우선순위
        1) 전일 종가
        2) 1분봉 VWAP
        3) 1분봉 최근 체결가
        """

        # 1) 전일 종가
        try:
            if hasattr(kis, "get_close_price"):
                close_px = _to_float(kis.get_close_price(code), None)
                if close_px and close_px > 0:
                    return float(close_px), "close"
        except Exception:
            pass

        # 2) 1분봉 데이터 기반 (VWAP → 최근 체결가)
        try:
            candles = _get_intraday_1min(kis, code, count=40)
            if candles:
                vwap_val = _compute_vwap_from_1min(candles)
                if vwap_val and vwap_val > 0:
                    return float(vwap_val), "vwap"

                last_close = _to_float(candles[-1].get("close"), None)
                if last_close and last_close > 0:
                    return float(last_close), "last_trade"
        except Exception as e:
            logger.warning(f"[PRICE_FALLBACK_FAIL] {code} 과거가 기반 대체값 실패: {e}")

        return None

    # 4) 히스토리 기반 대체값 시도
    fallback_px = _historical_fallback()
    if fallback_px:
        px_val, px_src = fallback_px
        if px_val and px_val > 0:
            return _store_and_return(float(px_val), px_src or "fallback", log_level=logging.INFO)

    # 5) 최후: 캐시가 있으면 stale_ok_sec 내 제공  (BUGFIX: px 반환)
    ent = _LAST_PRICE_CACHE.get(code)
    if ent and (now - ent["ts"] <= stale_ok_sec):
        src = ent.get("source") or "stale_cache"
        logger.info(f"[PRICE_SRC] {code} ← {src}({float(ent['px']):.2f}) (stale ok)")
        return (float(ent["px"]), src) if with_source else float(ent["px"])
    return None

def _fetch_balances(kis: KisAPI, ttl_sec: int = 15) -> List[Dict[str, Any]]:
    """
    get_balance / get_balance_all 호출을 15초 캐시.
    초당 루프를 돌려도 실제 API는 15초에 1번만 두드리도록 한다.
    """
    now = time.time()
    try:
        if _BALANCE_CACHE["balances"] and (now - float(_BALANCE_CACHE["ts"])) <= ttl_sec:
            return list(_BALANCE_CACHE["balances"])
    except Exception:
        pass

    if hasattr(kis, "get_balance_all"):
        res = _with_retry(kis.get_balance_all)
    else:
        res = _with_retry(kis.get_balance)

    if isinstance(res, dict):
        positions = res.get("positions") or res.get("output1") or []
        if not isinstance(positions, list):
            logger.error(f"[BAL_STD_FAIL] positions 타입 이상: {type(positions)}")
            positions = []
    elif isinstance(res, list):
        positions = res
    else:
        logger.error(f"[BAL_STD_FAIL] 지원하지 않는 반환 타입: {type(res)}")
        positions = []

    normalized: List[Dict[str, Any]] = []
    for row in positions:
        try:
            code = str(row.get("code") or row.get("pdno") or "").strip()
            if not code:
                continue
            qty = _to_int(row.get("qty") if "qty" in row else row.get("hldg_qty"))
            sell_psbl_qty = _to_int(
                row.get("sell_psbl_qty") if "sell_psbl_qty" in row else row.get("ord_psbl_qty")
            )
            if qty <= 0 and sell_psbl_qty > 0:
                qty = sell_psbl_qty
            avg_price = _to_float(
                row.get("avg_price") if "avg_price" in row else row.get("pchs_avg_pric")
            )

            normalized.append(
                {
                    "code": code.zfill(6),
                    "name": row.get("name") or row.get("prdt_name"),
                    "qty": qty,
                    "sell_psbl_qty": sell_psbl_qty,
                    "avg_price": avg_price,
                    "current_price": _to_float(row.get("prpr") or row.get("price")),
                    "eval_amount": _to_int(row.get("evlu_amt")),
                    "raw": row,
                }
            )
        except Exception as e:
            logger.warning(f"[BAL_STD_FAIL] 잔고 행 파싱 실패: {e}")
            continue

    _BALANCE_CACHE["ts"] = now
    _BALANCE_CACHE["balances"] = list(normalized)
    return normalized


def _get_effective_ord_cash(kis: KisAPI, soft_cap: int | float | None = None) -> int:
    """
    오늘 주문 가능 예수금을 가져오되,
    - 0 이하이거나
    - 조회 실패 / None
    이면 DAILY_CAPITAL을 fallback으로 사용한다.
    (모의투자에서 get_cash_available_today가 항상 0을 주는 경우 보호)
    soft_cap이 주어지면 조회된 값과 비교해 더 작은 값을 사용한다.
    """
    try:
        cash = kis.get_cash_available_today()
        if cash is None:
            raise ValueError("cash is None")
        cash = int(cash)
        logger.info(f"[BUDGET] today cash available(raw) = {cash:,} KRW")
    except Exception as e:
        logger.warning(
            f"[BUDGET] 예수금 조회 실패/무효({e}) → DAILY_CAPITAL {DAILY_CAPITAL:,}원 사용"
        )
        cash = DAILY_CAPITAL

    if cash <= 0:
        logger.warning(
            f"[BUDGET] today cash <= 0 → DAILY_CAPITAL {DAILY_CAPITAL:,}원 사용"
        )
        cash = DAILY_CAPITAL

    if soft_cap is not None:
        try:
            cap_int = int(float(soft_cap))
            if cap_int > 0:
                if cash > cap_int:
                    logger.info(
                        "[BUDGET] applying soft cap %s → cash clipped from %s to %s",
                        f"{cap_int:,}",
                        f"{cash:,}",
                        f"{cap_int:,}",
                    )
                cash = min(cash, cap_int)
        except Exception:
            logger.warning("[BUDGET] invalid soft_cap provided: %s", soft_cap)

    return cash


def _detect_pullback_reversal(
    kis: KisAPI,
    code: str,
    current_price: Optional[float] = None,
    lookback: int = PULLBACK_LOOKBACK,
    pullback_days: int = PULLBACK_DAYS,
    buffer_pct: float = PULLBACK_REVERSAL_BUFFER_PCT,
    reversal_buffer_pct: Optional[float] = None,  # ← 추가
) -> Dict[str, Any]:
    """
    신고가 달성 이후 3일 연속 하락 후 반등 여부를 판정한다.
    ...
    """
    # reversal_buffer_pct를 키워드로 받았으면 그 값을 우선 사용
    if reversal_buffer_pct is not None:
        buffer_pct = reversal_buffer_pct

    try:
        candles = _get_daily_candles_cached(
            kis, code, count=max(lookback, pullback_days + 5)
        )
    except Exception as e:
        return {"setup": False, "reason": f"daily_fetch_fail:{e}"}

    if len(candles) < pullback_days + 2:
        return {"setup": False, "reason": "not_enough_candles"}

    today = datetime.now(KST).strftime("%Y%m%d")
    completed = list(candles)
    if completed and str(completed[-1].get("date")) == today:
        completed = completed[:-1]
    if len(completed) < pullback_days + 2:
        return {"setup": False, "reason": "insufficient_history_after_trim"}

    window = completed[-lookback:]
    highs = [float(c.get("high") or 0.0) for c in window]
    if not highs:
        return {"setup": False, "reason": "no_high_data"}

    peak_price = max(highs)
    try:
        peak_idx = max(
            i for i, c in enumerate(window) if float(c.get("high") or 0.0) == peak_price
        )
    except Exception:
        return {"setup": False, "reason": "peak_index_error"}

    # 직전 일자까지 연속 하락 구간 길이를 계산(어제까지 n일 연속 하락인지)
    down_streak_len = 0
    last_idx = len(window) - 1
    while last_idx > peak_idx:
        try:
            cur_close = float(window[last_idx].get("close") or 0.0)
            prev_close = float(window[last_idx - 1].get("close") or 0.0)
        except Exception:
            break
        if cur_close <= 0 or prev_close <= 0:
            break
        if cur_close < prev_close:
            down_streak_len += 1
            last_idx -= 1
            continue
        break

    # [RELAX] 2일 연속 하락이면 완화 진입 허용, 또는 VWAP 회복 시 예외 허용
    vwap_reclaim = False
    if current_price:
        try:
            intra = _compute_intraday_entry_context(kis, code, slow=MOM_SLOW)
            vwap_val = intra.get("vwap")
            last_close = intra.get("last_close") or current_price
            if vwap_val and last_close:
                vwap_reclaim = float(last_close) >= float(vwap_val) * (1 - VWAP_TOL)
        except Exception:
            vwap_reclaim = False

    relaxed_streak_ok = down_streak_len >= pullback_days or down_streak_len >= 2
    if not relaxed_streak_ok and not vwap_reclaim:
        return {
            "setup": False,
            "peak_price": peak_price,
            "reason": "not_enough_consecutive_down",
        }

    if last_idx < peak_idx:
        return {
            "setup": False,
            "peak_price": peak_price,
            "reason": "down_streak_not_after_peak",
        }

    last_down = window[len(window) - 1]
    try:
        reversal_line = max(
            float(last_down.get("high") or 0.0), float(last_down.get("close") or 0.0)
        )
    except Exception:
        reversal_line = 0.0

    reversal_price = reversal_line * (1.0 + buffer_pct / 100.0)
    reversing = (
        current_price is not None
        and reversal_price > 0
        and float(current_price) >= float(reversal_price)
    )

    return {
        "setup": True,
        "reversing": bool(reversing),
        "reversal_price": float(reversal_price) if reversal_price > 0 else None,
        "peak_price": float(peak_price),
        "peak_date": window[peak_idx].get("date"),
        "last_down_date": last_down.get("date"),
    }


def _classify_champion_grade(info: Dict[str, Any]) -> str:
    trades = _to_int(info.get("trades"), 0)
    win = _to_float(info.get("win_rate_pct"), 0.0)
    mdd = abs(_to_float(info.get("mdd_pct"), 0.0) or 0.0)
    sharpe = _to_float(info.get("sharpe_m") or info.get("sharpe"), 0.0)
    cumret = _to_float(
        info.get("cumulative_return_pct") or info.get("avg_return_pct"), 0.0
    )
    turnover = _to_float(
        info.get("prev_turnover") or info.get("avg_turnover") or info.get("turnover"),
        0.0,
    )

    turnover_ok = turnover <= 0 or turnover >= CHAMPION_A_RULES["min_turnover"]
    if (
        trades >= CHAMPION_A_RULES["min_trades"]
        and cumret >= CHAMPION_A_RULES["min_cumret_pct"]
        and mdd <= CHAMPION_A_RULES["max_mdd_pct"]
        and win >= CHAMPION_A_RULES["min_win_pct"]
        and sharpe >= CHAMPION_A_RULES["min_sharpe"]
        and turnover_ok
    ):
        return "A"

    if (
        trades >= CHAMPION_MIN_TRADES
        and win >= CHAMPION_MIN_WINRATE
        and mdd <= CHAMPION_MAX_MDD
        and sharpe >= CHAMPION_MIN_SHARPE
    ):
        return "B"

    return "C"


def _compute_daily_entry_context(
    kis: KisAPI, code: str, current_price: Optional[float], price_source: Optional[str] = None
) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {"current_price": current_price, "price_source": price_source}
    try:
        candles = _get_daily_candles_cached(kis, code, count=max(PULLBACK_LOOKBACK, 60))
    except Exception:
        return ctx

    today = datetime.now(KST).strftime("%Y%m%d")
    completed = list(candles)
    if completed and str(completed[-1].get("date")) == today:
        completed = completed[:-1]

    if not completed:
        return ctx

    closes = [float(c.get("close") or 0.0) for c in completed if c.get("close")]
    highs = [float(c.get("high") or 0.0) for c in completed if c.get("high")]
    lows = [float(c.get("low") or 0.0) for c in completed if c.get("low")]

    if len(closes) >= 20:
        ma20 = sum(closes[-20:]) / 20.0
        ctx["ma20"] = ma20
        if current_price:
            ctx["ma20_ratio"] = current_price / ma20
            ctx["ma20_risk"] = max(0.0, current_price - ma20)

        # 단기/중기 추세 정배열 및 상승 여부
        if len(closes) >= 21:
            ma5 = sum(closes[-5:]) / 5.0
            ma10 = sum(closes[-10:]) / 10.0
            prev_ma20 = sum(closes[-21:-1]) / 20.0

            ctx["ma5"] = ma5
            ctx["ma10"] = ma10
            ctx["ma20_prev"] = prev_ma20

            bullish_stack = (
                ma5 > ma10 > ma20
                and ma20 > prev_ma20
                and float(closes[-1]) > ma20
            )
            ctx["strong_trend"] = bullish_stack

    strong_trend = bool(ctx.get("strong_trend"))
    effective_max_pullback = BAD_ENTRY_MAX_PULLBACK
    if strong_trend:
        effective_max_pullback = max(effective_max_pullback, 60.0)
    ctx["max_pullback_pct"] = effective_max_pullback

    if highs:
        window_60 = highs[-60:] if len(highs) >= 60 else highs
        peak_price = max(window_60)
        ctx["peak_price"] = peak_price
        if current_price and peak_price > 0:
            ctx["distance_to_peak"] = current_price / peak_price
            ctx["pullback_depth_pct"] = (peak_price - current_price) / peak_price * 100.0

    # 연속 하락 일수 체크 (신고가 이후 눌림 판단)
    down_streak = 0
    for idx in range(len(completed) - 1, 0, -1):
        cur = float(completed[idx].get("close") or 0.0)
        prev = float(completed[idx - 1].get("close") or 0.0)
        if cur <= 0 or prev <= 0:
            break
        if cur < prev:
            down_streak += 1
        else:
            break
    ctx["down_streak"] = down_streak

    try:
        atr = _get_atr(kis, code)
        if atr:
            ctx["atr"] = float(atr)
    except Exception:
        pass

    if closes and highs:
        recent_high = max(highs[-20:])
        ctx["recent_high_20"] = recent_high

        base_setup = bool(
            down_streak >= 2
            and ctx.get("pullback_depth_pct") is not None
            and ctx.get("pullback_depth_pct") >= GOOD_ENTRY_PULLBACK_RANGE[0]
            and (ctx.get("ma20_ratio") or 0) >= GOOD_ENTRY_MA20_RANGE[0]
            and recent_high >= max(highs[-60:]) * 0.95
        )

        relaxed_pullback_ok = (
            ctx.get("strong_trend")
            and ctx.get("pullback_depth_pct") is not None
            and ctx.get("pullback_depth_pct") >= GOOD_ENTRY_PULLBACK_RANGE[0]
            and ctx.get("pullback_depth_pct") <= float(ctx.get("max_pullback_pct") or 60.0)
            and (ctx.get("ma20_ratio") or 0) >= GOOD_ENTRY_MA20_RANGE[0]
        )

        ctx["setup_ok"] = bool(base_setup or relaxed_pullback_ok)
        if relaxed_pullback_ok and not base_setup:
            ctx["setup_reason"] = "strong_trend_relaxed"

    return ctx


def _compute_intraday_entry_context(
    kis: KisAPI,
    code: str,
    prev_high: Optional[float] = None,
    *,
    fast: Optional[int] = None,
    slow: Optional[int] = None,
) -> Dict[str, Any]:
    """
    진입 시점용 1분봉 VWAP / 박스 / 거래량 스파이크 컨텍스트 계산.

    prev_high는 이전 일자 고가(전일 high) 등 외부에서 넣어줄 수 있고,
    fast/slow는 모멘텀용 파라미터지만, 여기서는 주로 조회 길이 튜닝에 사용한다.
    """
    ctx: Dict[str, Any] = {}

    # intraday 1분봉 조회 길이 결정
    # - 기본은 120개
    # - slow가 들어오면 slow * 3 정도로 늘리되 최소 60개는 확보
    lookback = 120
    if slow is not None:
        try:
            slow_n = int(slow)
            lookback = max(slow_n * 3, 60)
        except (TypeError, ValueError):
            # 잘못 들어온 값이면 그냥 기본값 120 유지
            pass

    candles = _get_intraday_1min(kis, code, count=lookback)
    if not candles:
        return ctx

    vwap_val = _compute_vwap_from_1min(candles)
    ctx["vwap"] = vwap_val

    last = candles[-1]
    last_close = _to_float(last.get("close"), None)
    last_high = _to_float(last.get("high") or last.get("close"), None)
    last_low = _to_float(last.get("low") or last.get("close"), None)
    ctx["last_close"] = last_close
    ctx["last_high"] = last_high
    ctx["last_low"] = last_low

    if vwap_val and last_close:
        ctx["vwap_reclaim"] = last_close >= vwap_val

    highs = [
        float(c.get("high") or c.get("close") or 0.0)
        for c in candles
        if c.get("high") or c.get("close")
    ]
    lows = [
        float(c.get("low") or c.get("close") or 0.0)
        for c in candles
        if c.get("low") or c.get("close")
    ]
    vols = [float(c.get("volume") or 0.0) for c in candles]

    if highs:
        box_high = max(highs[-20:])
        box_low = min(lows[-20:]) if lows else None
        if last_high is not None and box_high:
            ctx["range_break"] = last_high >= box_high * 0.999
        if last_low is not None and box_low:
            ctx["box_floor"] = box_low

    if vols and len(vols) >= 10:
        recent_vol = sum(vols[-5:]) / 5.0
        base_vol = sum(vols[:-5]) / max(1, len(vols) - 5)
        if base_vol > 0:
            ctx["volume_spike"] = recent_vol >= base_vol * 1.5

    if vwap_val:
        below = sum(
            1 for c in candles if _to_float(c.get("close"), 0.0) < vwap_val
        )
        ctx["below_vwap_ratio"] = below / len(candles)

    if prev_high and last_high:
        ctx["prev_high_retest"] = last_high >= float(prev_high) * 0.999

    return ctx

def is_bad_entry(
    code: str,
    daily_ctx: Dict[str, Any],
    intraday_ctx: Dict[str, Any],
    regime_state: Optional[Dict[str, Any]] = None,
) -> bool:
    reasons = []
    strong_trend = bool(daily_ctx.get("strong_trend"))

    # 1) MA20 거리
    mr = daily_ctx.get("ma20_ratio")
    if mr is not None:
        try:
            mr_val = float(mr)
            if abs(mr_val) > BAD_ENTRY_MAX_MA20_DIST:
                reasons.append(f"MA20DIST {mr_val:.3f}")
        except:
            reasons.append("MA20DIST invalid")

    # 2) Pullback depth
    pb = daily_ctx.get("pullback_depth_pct")
    if pb is not None:
        try:
            pb_val = float(pb)
            max_pb = float(daily_ctx.get("max_pullback_pct") or BAD_ENTRY_MAX_PULLBACK)
            if pb_val > max_pb:
                reasons.append(f"PULLBACK {pb_val:.2f}")
        except:
            reasons.append("PULLBACK invalid")

    # 3) Regime drop
    if regime_state:
        drop = _to_float(regime_state.get("pct_change"), None)
        mode = regime_state.get("mode")
        if drop is not None and drop <= -2.5 and not (strong_trend and mode == "neutral"):
            reasons.append(f"REGIME_DROP {drop:.2f}")

    # 4) VWAP ratio
    bvr = intraday_ctx.get("below_vwap_ratio")
    if bvr is not None:
        try:
            bvr_val = float(bvr)
            if bvr_val >= BAD_ENTRY_MAX_BELOW_VWAP_RATIO:
                reasons.append(f"VWAP_RATIO {bvr_val:.2f}")
        except:
            reasons.append("VWAP_RATIO invalid")

    if reasons:
        logger.info(
            "[ENTRY-BAD] %s | 이유: %s | daily=%s intra=%s regime=%s",
            code,
            " / ".join(reasons),
            daily_ctx,
            intraday_ctx,
            regime_state,
        )
        return True

    logger.info(
        "[ENTRY-OK] %s | daily=%s intra=%s regime=%s",
        code,
        daily_ctx,
        intraday_ctx,
        regime_state,
    )
    return False


def is_good_entry(
    code: str,
    daily_ctx: Dict[str, Any],
    intraday_ctx: Dict[str, Any],
    prev_high: Optional[float] = None,
) -> bool:
    if not daily_ctx.get("setup_ok"):
        return False

    pullback = daily_ctx.get("pullback_depth_pct")
    strong_trend = bool(daily_ctx.get("strong_trend"))
    max_pb = float(daily_ctx.get("max_pullback_pct") or GOOD_ENTRY_PULLBACK_RANGE[1])
    if not strong_trend:
        max_pb = min(max_pb, GOOD_ENTRY_PULLBACK_RANGE[1])
    if pullback is None or not (
        GOOD_ENTRY_PULLBACK_RANGE[0] <= pullback <= max_pb
    ):
        return False

    ma20_ratio = daily_ctx.get("ma20_ratio")
    if ma20_ratio is None or not (
        GOOD_ENTRY_MA20_RANGE[0] <= ma20_ratio <= GOOD_ENTRY_MA20_RANGE[1]
    ):
        return False

    dist_peak = daily_ctx.get("distance_to_peak")
    if dist_peak is None or dist_peak > GOOD_ENTRY_MAX_FROM_PEAK:
        return False

    cur_px = daily_ctx.get("current_price")
    atr = daily_ctx.get("atr") or 0.0
    ma_risk = daily_ctx.get("ma20_risk") or 0.0
    risk = max(atr, ma_risk, (cur_px or 0) * 0.03)
    reward = max(0.0, (daily_ctx.get("peak_price") or 0) - (cur_px or 0)) + atr
    if risk <= 0 or reward / risk < GOOD_ENTRY_MIN_RR:
        return False

    signals = []
    if intraday_ctx.get("vwap_reclaim"):
        signals.append("vwap")
    if intraday_ctx.get("range_break"):
        signals.append("range")
    if intraday_ctx.get("volume_spike"):
        signals.append("volume")
    if prev_high and intraday_ctx.get("prev_high_retest"):
        signals.append("prev_high")

    return len(signals) >= GOOD_ENTRY_MIN_INTRADAY_SIG

# === [ANCHOR: INTRADAY_MOMENTUM] 1분봉 VWAP + 단기 모멘텀 ===
def _get_intraday_1min(kis: KisAPI, code: str, count: int = 60) -> List[Dict[str, Any]]:
    """
    KisAPI에 1분봉 메서드가 있으면 사용하고, 없으면 호환 메서드로 fallback.
    반환은 최소한 'close'와 'volume' 정보를 가진 dict 리스트라고 가정한다.
    """
    try:
        if hasattr(kis, "get_intraday_1min"):
            return kis.get_intraday_1min(code, count=count)
        if hasattr(kis, "get_minute_candles"):
            return kis.get_minute_candles(code, unit=1, count=count)
        if hasattr(kis, "get_intraday_candles"):
            return kis.get_intraday_candles(code, unit="1", count=count)
    except Exception as e:
        logger.warning(f"[INTRADAY_1M_FAIL] {code}: {e}")
    return []

def _compute_vwap_from_1min(candles: List[Dict[str, Any]]) -> Optional[float]:
    if not candles:
        return None
    pv = 0.0
    vol_sum = 0.0
    for c in candles:
        try:
            price = float(c.get("close") or c.get("trade_price") or c.get("price") or 0.0)
            vol = float(c.get("volume") or c.get("trade_volume") or 0.0)
        except Exception:
            continue
        if price <= 0 or vol <= 0:
            continue
        pv += price * vol
        vol_sum += vol
    if vol_sum <= 0:
        return None
    return pv / vol_sum

def _compute_intraday_momentum(candles: List[Dict[str, Any]], fast: int = MOM_FAST, slow: int = MOM_SLOW) -> float:
    closes: List[float] = []
    for c in candles:
        try:
            px = float(c.get("close") or c.get("trade_price") or c.get("price") or 0.0)
        except Exception:
            continue
        if px > 0:
            closes.append(px)
    if len(closes) < max(fast, slow):
        return 0.0
    fast_ma = sum(closes[-fast:]) / float(fast)
    slow_ma = sum(closes[-slow:]) / float(slow)
    if slow_ma <= 0:
        return 0.0
    return (fast_ma - slow_ma) / slow_ma * 100.0

def is_strong_momentum_vwap(kis: KisAPI, code: str) -> bool:
    """
    1분봉 VWAP + 단기 모멘텀 기반 모멘텀 강세 판정.
    - 최근 가격이 VWAP 위
    - fast/slow 모멘텀 >= MOM_TH_PCT
    """
    try:
        if hasattr(kis, "is_market_open") and not kis.is_market_open() and not ALLOW_WHEN_CLOSED:
            return False
    except Exception:
        pass

    candles = _get_intraday_1min(kis, code, count=max(MOM_SLOW * 3, 60))
    if not candles:
        return False

    try:
        last_candle = candles[-1]
        last_price = float(last_candle.get("close") or last_candle.get("trade_price") or last_candle.get("price") or 0.0)
    except Exception:
        return False
    if last_price <= 0:
        return False

    vwap_val = _compute_vwap_from_1min(candles)
    if vwap_val is None or vwap_val <= 0:
        return False

    mom = _compute_intraday_momentum(candles)
    strong = (last_price > vwap_val) and (mom >= MOM_TH_PCT)
    if strong:
        logger.info(
            f"[모멘텀 강세] {code}: 강한 상승추세, 능동관리 매도 보류 "
            f"(VWAP/1분봉 기준, last={last_price:.2f}, vwap={vwap_val:.2f}, mom={mom:.2f}%)"
        )
    return strong

# === 20D 수익률 ===
def get_20d_return_pct(kis: KisAPI, code: str) -> Optional[float]:
    try:
        if not kis.is_market_open() and not ALLOW_WHEN_CLOSED:
            raise NetTemporaryError("market closed skip")
    except Exception:
        pass

    MAX_RETRY = 3
    last_err: Optional[Exception] = None

    for attempt in range(1, MAX_RETRY + 1):
        try:
            candles = _get_daily_candles_cached(kis, code, count=21)
            if not candles or len(candles) < 21:
                raise DataShortError("need at least 21 candles")

            if any(('close' not in c) or (c['close'] is None) for c in candles):
                logger.error("[20D_RETURN_FAIL] %s: 캔들 close 결측", code)
                raise DataEmptyError("close missing")

            old = float(candles[-21]['close'])
            nowp = float(candles[-1]['close'])
            return ((nowp - old) / old) * 100.0

        except NetTemporaryError as e:
            last_err = e
            logger.warning("[CANDLE_TEMP_SKIP] %s 20D 계산 네트워크 실패 (재시도 %d/%d)", code, attempt, MAX_RETRY)
            time.sleep(1.0 * attempt)
            continue
        except DataEmptyError:
            logger.warning("[DATA_EMPTY] %s 0캔들(20D 계산 불가) - 상위에서 재확인/제외 판단", code)
            raise
        except DataShortError:
            logger.warning("[DATA_SHORT] %s 21개 미만(20D 계산 불가) - 상위에서 제외 판단", code)
            raise
        except Exception as e:
            last_err = e
            logger.warning("[20D_RETURN_FAIL] %s: 예외 %s (재시도 %d/%d)", code, e, attempt, MAX_RETRY)
            time.sleep(1.0 * attempt)
            continue

    if last_err:
        logger.warning("[20D_RETURN_FAIL] %s 최종 실패: %s", code, last_err)
    raise NetTemporaryError("20D return calc failed")

def is_strong_momentum(kis: KisAPI, code: str) -> bool:
    """
    기존 일봉 기반 모멘텀 대신,
    1분봉 VWAP + 단기 모멘텀 기준으로 강세를 판별한다.
    """
    return is_strong_momentum_vwap(kis, code)

def _percentile_rank(values: List[float], value: float, higher_is_better: bool = True) -> float:
    if not values:
        return 0.0
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return 0.0

    if higher_is_better:
        count = sum(1 for v in vals if v <= value)
    else:
        count = sum(1 for v in vals if v >= value)
    return (count / len(vals)) * 100.0

def _has_bullish_trend_structure(kis: KisAPI, code: str) -> Tuple[bool, Dict[str, float]]:
    """
    보유 지속 여부 판단용: 5/10/20일선 정배열 + 20일선 상승 + 종가>20일선 체크.
    """
    candles = _get_daily_candles_cached(kis, code, count=25)
    if not candles or len(candles) < 21:
        raise DataShortError("not enough candles")

    today = datetime.now(KST).strftime("%Y%m%d")
    completed = list(candles)
    if completed and str(completed[-1].get("date")) == today:
        completed = completed[:-1]

    if len(completed) < 21:
        raise DataShortError("insufficient completed candles")

    closes: List[float] = []
    for c in completed:
        close = c.get("close")
        if close is None:
            raise DataEmptyError("close missing")
        closes.append(float(close))

    if len(closes) < 21:
        raise DataShortError("need at least 21 closes")

    ma5 = sum(closes[-5:]) / 5.0
    ma10 = sum(closes[-10:]) / 10.0
    ma20 = sum(closes[-20:]) / 20.0
    prev_ma20 = sum(closes[-21:-1]) / 20.0
    last_close = closes[-1]

    aligned = last_close > ma20 and ma5 > ma10 > ma20 and ma20 > prev_ma20
    return aligned, {
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma20_prev": prev_ma20,
        "last_close": last_close,
    }

def _weight_to_qty(
    kis: KisAPI,
    code: str,
    weight: float,
    daily_capital: int,
    ref_price: Optional[float] = None
) -> int:
    weight = max(W_MIN_ONE, min(max(0.0, float(weight)), W_MAX_ONE))
    alloc = int(round(daily_capital * weight))

    price = None
    if ref_price is not None and float(ref_price) > 0:
        price = float(ref_price)

    if price is None:
        try:
            if kis.is_market_open():
                price = _safe_get_price(kis, code)
            else:
                if hasattr(kis, "get_close_price"):
                    try:
                        price = float(kis.get_close_price(code))
                    except Exception:
                        price = None
        except Exception:
            price = None

    if price is None or price <= 0:
        return 0

    return max(0, int(alloc // int(price)))



def _notional_to_qty(
    kis: KisAPI,
    code: str,
    notional: int,
    ref_price: Optional[float] = None
) -> int:
    """Target Notional(원)을 기준으로 수량 계산 (weight 클램프 없이 직접 계산)."""
    try:
        notional = int(notional)
    except Exception:
        return 0
    if notional <= 0:
        return 0

    price = None
    if ref_price is not None:
        try:
            if float(ref_price) > 0:
                price = float(ref_price)
        except Exception:
            price = None

    if price is None:
        try:
            if kis.is_market_open():
                price = _safe_get_price(kis, code)
            else:
                if hasattr(kis, "get_close_price"):
                    try:
                        price = float(kis.get_close_price(code))
                    except Exception:
                        price = None
        except Exception:
            price = None

    if price is None or price <= 0:
        return 0

    return max(0, int(notional // int(price)))
# === ATR, 상태 초기화 ===
def _get_atr(kis: KisAPI, code: str, window: int = 14) -> Optional[float]:
    if hasattr(kis, "get_atr"):
        try:
            return kis.get_atr(code, window=window)
        except Exception as e:
            logger.warning(f"[ATR_FAIL] {code}: {e}")
            return None
    return None

