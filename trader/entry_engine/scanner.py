from __future__ import annotations

import json
import logging
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
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


def scan_entry_candidates(
    *,
    watchlist: list[dict[str, Any]],
    ohlcv_provider: Callable[..., pd.DataFrame | None],
    precomputed_final30_df: pd.DataFrame | None = None,
    trade_precomputed_only: bool = False,
    data_metrics: dict[str, int] | None = None,
) -> dict[str, list[EntrySignal]]:
    logger.info("[ENTRY_ENGINE][COMPAT] scan_all_strategies -> trader.entry_engine.scanner.scan_entry_candidates")
    logger.info("[ENTRY_SCAN][ALL] starting all strategies scan on %s symbols", len(watchlist))

    required_scored_cols = {
        "code",
        "score_final",
        "tech_score",
        "breakout_score",
        "pullback_score",
        "momentum_score",
        "rs_percentile",
        "vcp_score",
        "entry_style_selected",
    }

    breakout: list[EntrySignal] = []
    pullback: list[EntrySignal] = []
    momentum: list[EntrySignal] = []
    merged: dict[str, EntrySignal] = {}

    per_symbol_rejects: dict[str, list[str]] = {}
    rejected_counts: Counter[str] = Counter()
    style_counts: Counter[str] = Counter()

    precomputed_map: dict[str, dict[str, Any]] = {}
    if precomputed_final30_df is not None and not precomputed_final30_df.empty and "code" in precomputed_final30_df.columns:
        for row in precomputed_final30_df.to_dict(orient="records"):
            code_key = str((row or {}).get("code") or "").zfill(6)
            if code_key:
                precomputed_map[code_key] = dict(row or {})

    required_precomputed_cols = {
        "breakout_score",
        "pullback_score",
        "momentum_score",
        "ma20",
        "ma50",
        "pullback_pct",
        "rs_percentile",
        "vcp_score",
    }

    def _metric_inc(key: str, delta: int = 1) -> None:
        if data_metrics is None:
            return
        data_metrics[key] = int(data_metrics.get(key, 0)) + int(delta)

    for item in watchlist:
        code = str(item.get("code") or "").zfill(6)
        if not code:
            continue
        name = str(item.get("name") or "")
        reasons: list[str] = []

        missing_scored = sorted([c for c in required_scored_cols if c not in item])
        if missing_scored:
            reasons.append("missing_scored_input")

        precomputed_row = precomputed_map.get(code)
        has_precomputed = bool(precomputed_row) and required_precomputed_cols.issubset(set(precomputed_row.keys()))
        if has_precomputed:
            logger.info("[ENTRY_SCAN][PRECOMPUTED_HIT] code=%s skip_long_ohlcv=1", code)
            _metric_inc("precomputed_hits", 1)

            close = _safe_float(precomputed_row.get("close", item.get("close", 0.0)))
            ma20 = _safe_float(precomputed_row.get("ma20", 0.0))
            ma50 = _safe_float(precomputed_row.get("ma50", 0.0))
            rs_percentile = _safe_float(precomputed_row.get("rs_percentile", item.get("rs_percentile", 0.0)))
            atr_pct = _safe_float(precomputed_row.get("atr_pct", item.get("atr_pct", 0.0)))
            pullback_pct_raw = _safe_float(precomputed_row.get("pullback_pct", 0.0))
            pullback_pct = pullback_pct_raw / 100.0 if pullback_pct_raw > 1.0 else pullback_pct_raw
            high_52w = _safe_float(precomputed_row.get("high_52w", 0.0))
            high_50 = _safe_float(precomputed_row.get("high_50", 0.0))
            vol_avg20 = _safe_float(precomputed_row.get("volume_avg20", 0.0))
            vol = _safe_float(precomputed_row.get("volume", 0.0))

            breakout_ok = _safe_float(precomputed_row.get("breakout_score", 0.0)) > 0.0
            pullback_ok = _safe_float(precomputed_row.get("pullback_score", 0.0)) > 0.0
            momentum_ok = _safe_float(precomputed_row.get("momentum_score", 0.0)) > 0.0
        else:
            request_days = 260
            if trade_precomputed_only and request_days > 60:
                logger.warning(
                    "[ENTRY_SCAN][LONG_OHLCV_BLOCKED] code=%s requested_days=%s source=trade_precomputed_only",
                    code,
                    request_days,
                )
                _metric_inc("long_fetch_blocked_count", 1)
                request_days = 60

            try:
                df = ohlcv_provider(
                    code,
                    request_days,
                    usage_context="trade" if trade_precomputed_only else None,
                    allow_long_fetch=not trade_precomputed_only,
                    purpose="entry_scan",
                )
                _metric_inc("short_fetch_count", 1)
            except TypeError:
                # Backward compatible call shape.
                df = ohlcv_provider(code, request_days)
                _metric_inc("short_fetch_count", 1)
            except Exception:
                df = None
                reasons.append("price_data_missing")

            if df is None or df.empty:
                if "price_data_missing" not in reasons:
                    reasons.append("price_data_missing")
                per_symbol_rejects[code] = reasons
                rejected_counts.update(reasons)
                continue

            if len(df) < 50:
                reasons.append("db_ohlcv_insufficient")

            try:
                close = _safe_float(df["close"].iloc[-1])
                high_50 = _safe_float(df["high"].tail(50).max())
                ma20 = _safe_float(df["close"].rolling(window=20, min_periods=1).mean().iloc[-1])
                ma50 = _safe_float(df["close"].rolling(window=50, min_periods=1).mean().iloc[-1])
                vol_avg20 = _safe_float(df["volume"].tail(20).mean())
                vol = _safe_float(df["volume"].iloc[-1])
                high_52w = _safe_float(df["high"].tail(min(252, len(df))).max())
                pullback_pct = (high_52w - close) / high_52w if high_52w > 0 else 0.0
                rs_percentile = _safe_float(item.get("rs_percentile", 0.0))
                atr_pct = _safe_float(item.get("atr_pct", 0.0))
            except Exception:
                reasons.append("price_data_missing")
                per_symbol_rejects[code] = reasons
                rejected_counts.update(reasons)
                continue

            breakout_ok = close > high_50 and vol > (vol_avg20 * 1.5 if vol_avg20 > 0 else 0)
            pullback_ok = close > ma50 and 0.05 <= pullback_pct <= 0.15
            momentum_ok = rs_percentile >= 80 and close > ma20 and vol > (vol_avg20 if vol_avg20 > 0 else 0)

        if not breakout_ok:
            reasons.append("breakout_condition_fail")
        if not pullback_ok:
            reasons.append("pullback_condition_fail")
        if not momentum_ok:
            reasons.append("momentum_condition_fail")
        if atr_pct > 12.0:
            reasons.append("atr_exceed")
        if not (0.02 <= pullback_pct <= 0.20):
            reasons.append("pullback_range_fail")
        if vol_avg20 <= 0:
            reasons.append("volume_condition_fail")

        signal: EntrySignal | None = None
        if breakout_ok:
            signal = EntrySignal(
                code=code,
                name=name,
                strategy="breakout",
                close=close,
                signal_strength=min(1.0, max(0.0, (close - high_50) / high_50 * 10.0)) if high_50 > 0 else 0.0,
                meta={"high_50": high_50, "volume": vol, "volume_avg20": vol_avg20},
            )
            breakout.append(signal)
            style_counts["breakout"] += 1
        elif pullback_ok:
            signal = EntrySignal(
                code=code,
                name=name,
                strategy="pullback",
                close=close,
                signal_strength=max(0.0, 1.0 - abs(pullback_pct - 0.10) / 0.05),
                meta={"ma50": ma50, "high_52w": high_52w, "pullback_pct": pullback_pct * 100.0},
            )
            pullback.append(signal)
            style_counts["pullback"] += 1
        elif momentum_ok:
            signal = EntrySignal(
                code=code,
                name=name,
                strategy="momentum",
                close=close,
                signal_strength=min(1.0, (rs_percentile - 80) / 20),
                meta={"rs_percentile": rs_percentile, "ma20": ma20, "volume": vol, "volume_avg20": vol_avg20},
            )
            momentum.append(signal)
            style_counts["momentum"] += 1

        if signal is not None:
            prev = merged.get(signal.code)
            if prev is None or signal.signal_strength > prev.signal_strength:
                merged[signal.code] = signal
            continue

        uniq_reasons = sorted(set(reasons))
        per_symbol_rejects[code] = uniq_reasons
        rejected_counts.update(uniq_reasons)

    all_signals = list(merged.values())
    passed = len(all_signals)
    total = len([w for w in watchlist if w.get("code")])

    logger.info(
        "[ENTRY_SCAN][SUMMARY] total=%s passed=%s rejected_counts=%s",
        total,
        passed,
        dict(rejected_counts),
    )

    top_rejects_n = int(os.getenv("ENTRY_SCAN_LOG_TOP_REJECTS", "10") or "10")
    if passed == 0 and per_symbol_rejects:
        ranked_rejects = sorted(
            per_symbol_rejects.items(),
            key=lambda kv: (len(kv[1]), kv[0]),
        )
        for code, reasons in ranked_rejects[:top_rejects_n]:
            logger.info("[ENTRY_SCAN][REJECT_TOP] code=%s reasons=%s", code, reasons)

    near_miss_candidates: list[dict[str, Any]] = []
    force_diag = os.getenv("TRADE_FORCE_MIN1_DIAG", "1") == "1"
    if force_diag and passed == 0 and per_symbol_rejects:
        ranked_rejects = sorted(per_symbol_rejects.items(), key=lambda kv: (len(kv[1]), kv[0]))
        for code, reasons in ranked_rejects[:3]:
            near_miss_candidates.append({"code": code, "missing": reasons})
            logger.info("[ENTRY_SCAN][NEAR_MISS] code=%s missing=%s", code, reasons)

    if os.getenv("ENTRY_SCAN_SAVE_DEBUG", "1") == "1":
        trade_date = (os.getenv("TRADE_DATE") or datetime.now().strftime("%Y-%m-%d"))
        path = Path("runtime") / "diagnostics" / trade_date / "entry_scan_debug.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "source_final30": "watchlist",
            "scan_input_codes": [str(w.get("code") or "").zfill(6) for w in watchlist if w.get("code")],
            "effective_filters": {
                "required_scored_cols": sorted(required_scored_cols),
                "top_rejects_n": top_rejects_n,
            },
            "per_symbol_rejection_reasons": per_symbol_rejects,
            "aggregate_rejected_counts": dict(rejected_counts),
            "candidate_counts_by_style": dict(style_counts),
            "near_miss_candidates": near_miss_candidates,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "[ENTRY_SCAN][ALL] completed - breakout=%s pullback=%s momentum=%s unique=%s",
        len(breakout),
        len(pullback),
        len(momentum),
        len(all_signals),
    )
    return {"breakout": breakout, "pullback": pullback, "momentum": momentum, "all": all_signals}
