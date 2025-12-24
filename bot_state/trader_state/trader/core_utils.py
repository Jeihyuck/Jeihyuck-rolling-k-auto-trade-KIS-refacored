# -*- coding: utf-8 -*-
"""기초 유틸리티와 상태 관리 함수 모음."""
from __future__ import annotations

import json
import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .core_constants import (
    LOG_DIR,
    STATE_FILE,
    STATE_WEEKLY_PATH,
    WEEKLY_ANCHOR_REF,
    _cfg,
    _this_iso_week_key,
    KST,
    REBALANCE_ANCHOR,
    logger,
)
from .kis_wrapper import KisAPI
from .code_utils import normalize_code
from .state_io import atomic_write_json

__all__ = [
    "_krx_tick",
    "_round_to_tick",
    "get_market",
    "_read_last_weekly",
    "_write_last_weekly",
    "should_weekly_rebalance_now",
    "stamp_weekly_done",
    "get_rebalance_anchor_date",
    "log_trade",
    "save_state",
    "save_state_atomic",
    "load_state",
    "_with_retry",
    "_to_int",
    "_to_float",
    "_log_realized_pnl",
    "_get_daily_candles_cached",
]


def _krx_tick(price: float) -> int:
    p = float(price or 0)
    if p >= 500_000:
        return 1_000
    if p >= 100_000:
        return 500
    if p >= 50_000:
        return 100
    if p >= 10_000:
        return 50
    if p >= 5_000:
        return 10
    if p >= 1_000:
        return 5
    return 1


def _round_to_tick(price: float, mode: str = "nearest") -> int:
    """mode: 'down' | 'up' | 'nearest'"""
    if price is None or price <= 0:
        return 0
    tick = _krx_tick(price)
    q = price / tick
    if mode == "down":
        q = int(q)
    elif mode == "up":
        q = int(q) if q == int(q) else int(q) + 1
    else:
        q = int(q + 0.5)
    return int(q * tick)


def get_market(code: str) -> str:
    from .core_constants import MARKET_MAP

    return MARKET_MAP.get(code, "J")


# === [ANCHOR: DAILY_CANDLE_CACHE] 일봉 완전 캐싱 ===
_DAILY_CANDLE_CACHE: Dict[str, Dict[str, Any]] = {}


def _get_daily_candles_cached(kis: KisAPI, code: str, count: int) -> List[Dict[str, Any]]:
    """
    코드별 일봉을 당일 기준으로 캐싱.
    - 동일 코드/거래일에서는 최초 요청 시에만 API 호출
    - 이후 더 긴 count가 들어오면 한 번 더 호출해서 캐시 갱신
    """

    today = datetime.now(KST).date()
    code_key = normalize_code(code)
    if not code_key:
        return []
    entry = _DAILY_CANDLE_CACHE.get(code_key)
    if entry and entry.get("date") == today and len(entry.get("candles") or []) >= count:
        return entry["candles"]

    candles = kis.get_daily_candles(code_key, count=count)
    if candles:
        _DAILY_CANDLE_CACHE[code_key] = {"date": today, "candles": candles}
    return candles or []


def _read_last_weekly():
    if not STATE_WEEKLY_PATH.exists():
        return None
    try:
        return (json.loads(STATE_WEEKLY_PATH.read_text(encoding="utf-8"))).get(
            "weekly_rebalanced_at"
        )
    except Exception:
        return None


def _write_last_weekly(now=None):
    now = now or datetime.now(KST)
    try:
        STATE_WEEKLY_PATH.write_text(
            json.dumps({"weekly_rebalanced_at": _this_iso_week_key(now)}, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning(f"[STATE_WRITE_FAIL] weekly: {e}")


def should_weekly_rebalance_now(now=None):
    """
    규칙:
      - 이번 주에 아직 리밸런싱 기록이 없으면 True
      - FORCE_WEEKLY_REBALANCE=1 이면 시간/요일 무시하고 True (단 1회)
    """
    now = now or datetime.now(KST)
    force = _cfg("FORCE_WEEKLY_REBALANCE") == "1"
    last = _read_last_weekly()
    cur = _this_iso_week_key(now)
    if force:
        logger.info("[REBALANCE] FORCE_WEEKLY_REBALANCE=1 → 주간 리밸런싱 강제 트리거")
        return True
    if last != cur:
        return True
    return False


def stamp_weekly_done(now=None):
    _write_last_weekly(now)


def get_rebalance_anchor_date(now: Optional[datetime] = None) -> str:
    """
    weekly 모드에서 기준일 산정:
      - WEEKLY_ANCHOR_REF='last'  → 직전 일요일(기본)
      - WEEKLY_ANCHOR_REF='next'  → 다음 일요일
    """
    now = now or datetime.now(KST)
    today = now.date()

    if REBALANCE_ANCHOR == "weekly":
        ref = WEEKLY_ANCHOR_REF if WEEKLY_ANCHOR_REF in ("last", "next", "prev", "previous") else "last"
        if ref in ("last", "prev", "previous"):
            days_since_sun = (today.weekday() + 1) % 7
            anchor_date = today - timedelta(days=days_since_sun)
        else:
            days_to_sun = (6 - today.weekday()) % 7
            anchor_date = today + timedelta(days=days_to_sun)
        return anchor_date.strftime("%Y-%m-%d")

    if REBALANCE_ANCHOR == "today":
        return today.strftime("%Y-%m-%d")

    return today.replace(day=1).strftime("%Y-%m-%d")


def log_trade(trade: dict) -> None:
    today = datetime.now(KST).strftime("%Y-%m-%d")
    logfile = LOG_DIR / f"trades_{today}.json"
    with open(logfile, "a", encoding="utf-8") as f:
        payload = dict(trade)
        if "code" in payload:
            payload["code"] = normalize_code(payload.get("code"))
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def save_state_atomic(holding: Dict[str, Any], traded: Dict[str, Any]) -> None:
    payload = {"holding": holding, "traded": traded}
    atomic_write_json(STATE_FILE, payload)


def save_state(holding: Dict[str, Any], traded: Dict[str, Any]) -> None:
    save_state_atomic(holding, traded)


def load_state() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            return state.get("holding", {}), state.get("traded", {})
        except Exception as e:
            logger.warning("[STATE] failed to load %s: %s", STATE_FILE, e)
    return {}, {}


def _with_retry(func, *args, max_retries=5, base_delay=0.6, **kwargs):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            sleep_sec = base_delay * (1.6 ** (attempt - 1)) + random.uniform(0, 0.25)
            logger.error(
                f"[재시도 {attempt}/{max_retries}] {func.__name__} 실패: {e} → {sleep_sec:.2f}s 대기 후 재시도"
            )
            time.sleep(sleep_sec)
    raise last_err


def _to_int(val, default=0) -> int:
    try:
        return int(float(val))
    except Exception:
        return default


def _to_float(val, default=None) -> Optional[float]:
    try:
        return float(val)
    except Exception:
        return default


def _log_realized_pnl(
    code: str,
    exec_px: Optional[float],
    sell_qty: int,
    buy_price: Optional[float],
    reason: str = "",
) -> None:
    try:
        if exec_px is None or sell_qty <= 0 or not buy_price or buy_price <= 0:
            return
        pnl_pct = ((float(exec_px) - float(buy_price)) / float(buy_price)) * 100.0
        profit = (float(exec_px) - float(buy_price)) * int(sell_qty)
        msg = (
            f"[P&L] {code} SELL {int(sell_qty)}@{float(exec_px):.2f} / BUY={float(buy_price):.2f} "
            f"→ PnL={pnl_pct:.2f}% (₩{int(round(profit)):,.0f})"
        )
        if reason:
            msg += f" / REASON={reason}"
        logger.info(msg)
    except Exception as e:
        logger.warning(f"[P&L_LOG_FAIL] {code} err={e}")
