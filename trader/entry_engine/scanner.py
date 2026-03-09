from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class EntrySignal:
    code: str
    name: str
    strategy: str
    close: float
    signal_strength: float
    meta: dict[str, Any]


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None:
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _scan_breakout(*, watchlist: list[dict[str, Any]], ohlcv_provider: Callable[[str, int], pd.DataFrame | None]) -> list[EntrySignal]:
    signals: list[EntrySignal] = []
    for item in watchlist:
        code = str(item.get("code") or "").zfill(6)
        if not code:
            continue
        name = str(item.get("name") or "")
        try:
            df = ohlcv_provider(code, 60)
            if df is None or len(df) < 50:
                continue
            close = _safe_float(df["close"].iloc[-1])
            high_50 = _safe_float(df["high"].tail(50).max())
            vol_avg20 = _safe_float(df["volume"].tail(20).mean())
            vol = _safe_float(df["volume"].iloc[-1])
            if close > high_50 and vol > (vol_avg20 * 1.5):
                breakout_strength = min(1.0, (close - high_50) / high_50 * 10.0) if high_50 > 0 else 0.0
                volume_strength = min(1.0, vol / (vol_avg20 * 1.5)) if vol_avg20 > 0 else 0.0
                signals.append(
                    EntrySignal(
                        code=code,
                        name=name,
                        strategy="breakout",
                        close=close,
                        signal_strength=(breakout_strength * 0.6 + volume_strength * 0.4),
                        meta={"high_50": high_50, "volume": vol, "volume_avg20": vol_avg20},
                    )
                )
        except Exception:
            continue
    return signals


def _scan_pullback(*, watchlist: list[dict[str, Any]], ohlcv_provider: Callable[[str, int], pd.DataFrame | None]) -> list[EntrySignal]:
    signals: list[EntrySignal] = []
    for item in watchlist:
        code = str(item.get("code") or "").zfill(6)
        if not code:
            continue
        name = str(item.get("name") or "")
        try:
            df = ohlcv_provider(code, 260)
            if df is None or len(df) < 50:
                continue
            close = _safe_float(df["close"].iloc[-1])
            ma50 = _safe_float(df["close"].rolling(window=50, min_periods=1).mean().iloc[-1])
            high_52w = _safe_float(df["high"].tail(min(252, len(df))).max())
            pullback_pct = (high_52w - close) / high_52w if high_52w > 0 else 0.0
            if close > ma50 and 0.05 <= pullback_pct <= 0.15:
                ma_strength = min(1.0, (close - ma50) / ma50 * 10.0) if ma50 > 0 else 0.0
                pullback_optimality = max(0.0, 1.0 - abs(pullback_pct - 0.10) / 0.05)
                signals.append(
                    EntrySignal(
                        code=code,
                        name=name,
                        strategy="pullback",
                        close=close,
                        signal_strength=(ma_strength * 0.4 + pullback_optimality * 0.6),
                        meta={"ma50": ma50, "high_52w": high_52w, "pullback_pct": pullback_pct * 100.0},
                    )
                )
        except Exception:
            continue
    return signals


def _scan_momentum(*, watchlist: list[dict[str, Any]], ohlcv_provider: Callable[[str, int], pd.DataFrame | None]) -> list[EntrySignal]:
    signals: list[EntrySignal] = []
    for item in watchlist:
        code = str(item.get("code") or "").zfill(6)
        if not code:
            continue
        name = str(item.get("name") or "")
        rs_percentile = _safe_float(item.get("rs_pctile", item.get("rs_percentile", 0.0)))
        try:
            df = ohlcv_provider(code, 60)
            if df is None or len(df) < 20:
                continue
            close = _safe_float(df["close"].iloc[-1])
            ma20 = _safe_float(df["close"].rolling(window=20, min_periods=1).mean().iloc[-1])
            vol_avg20 = _safe_float(df["volume"].tail(20).mean())
            vol = _safe_float(df["volume"].iloc[-1])
            if rs_percentile >= 80 and close > ma20 and vol > vol_avg20:
                rs_strength = min(1.0, (rs_percentile - 80) / 20)
                ma_strength = min(1.0, (close - ma20) / ma20 * 20.0) if ma20 > 0 else 0.0
                volume_strength = min(1.0, vol / vol_avg20 - 1.0) if vol_avg20 > 0 else 0.0
                signals.append(
                    EntrySignal(
                        code=code,
                        name=name,
                        strategy="momentum",
                        close=close,
                        signal_strength=(rs_strength * 0.5 + ma_strength * 0.3 + volume_strength * 0.2),
                        meta={"rs_percentile": rs_percentile, "ma20": ma20, "volume": vol, "volume_avg20": vol_avg20},
                    )
                )
        except Exception:
            continue
    return signals


def scan_entry_candidates(*, watchlist: list[dict[str, Any]], ohlcv_provider: Callable[[str, int], pd.DataFrame | None]) -> dict[str, list[EntrySignal]]:
    logger.info("[ENTRY_ENGINE][COMPAT] scan_all_strategies -> trader.entry_engine.scanner.scan_entry_candidates")
    logger.info("[ENTRY_SCAN][ALL] starting all strategies scan on %s symbols", len(watchlist))
    breakout = _scan_breakout(watchlist=watchlist, ohlcv_provider=ohlcv_provider)
    pullback = _scan_pullback(watchlist=watchlist, ohlcv_provider=ohlcv_provider)
    momentum = _scan_momentum(watchlist=watchlist, ohlcv_provider=ohlcv_provider)
    merged: dict[str, EntrySignal] = {}
    for signal in breakout + pullback + momentum:
        prev = merged.get(signal.code)
        if prev is None or signal.signal_strength > prev.signal_strength:
            merged[signal.code] = signal
    all_signals = list(merged.values())
    logger.info(
        "[ENTRY_SCAN][ALL] completed - breakout=%s pullback=%s momentum=%s unique=%s",
        len(breakout),
        len(pullback),
        len(momentum),
        len(all_signals),
    )
    return {"breakout": breakout, "pullback": pullback, "momentum": momentum, "all": all_signals}
