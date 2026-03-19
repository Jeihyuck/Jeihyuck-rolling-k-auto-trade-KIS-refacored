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

from trader.decision_schema import build_entry_evaluation, normalize_entry_setup_family

logger = logging.getLogger(__name__)

BREAKOUT_MIN_SCORE = float(os.getenv("ENTRY_SCAN_BREAKOUT_MIN_SCORE", "60") or "60")
PULLBACK_MIN_SCORE = float(os.getenv("ENTRY_SCAN_PULLBACK_MIN_SCORE", "55") or "55")
MOMENTUM_MIN_SCORE = float(os.getenv("ENTRY_SCAN_MOMENTUM_MIN_SCORE", "60") or "60")


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
    epsilon = 1e-6
    breakout_pass_count = 0
    pullback_pass_count = 0
    momentum_pass_count = 0
    setup_ok_count = 0
    multi_pass_count = 0
    none_pass_count = 0
    evaluations: list[dict[str, Any]] = []

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

            pivot_price = _safe_float(precomputed_row.get("pivot_price", precomputed_row.get("pivot", 0.0)))
            breakout_score = _safe_float(precomputed_row.get("breakout_score", 0.0))
            pullback_score = _safe_float(precomputed_row.get("pullback_score", 0.0))
            momentum_score = _safe_float(precomputed_row.get("momentum_score", 0.0))
            breakout_strength = breakout_score / 100.0
            pullback_strength = pullback_score / 100.0
            momentum_strength = momentum_score / 100.0
            breakout_context_ok = (
                breakout_score >= BREAKOUT_MIN_SCORE
                and breakout_strength > epsilon
                and close > 0
                and ((pivot_price > 0 and close >= pivot_price) or (high_50 > 0 and close >= high_50 * 0.995))
                and vol_avg20 > 0
                and vol >= (vol_avg20 * 1.1)
            )
            pullback_context_ok = (
                pullback_score >= PULLBACK_MIN_SCORE
                and pullback_strength > epsilon
                and close > 0
                and ma20 > 0
                and ma50 > 0
                and close >= min(ma20, ma50)
                and 0.03 <= pullback_pct <= 0.18
            )
            momentum_context_ok = (
                momentum_score >= MOMENTUM_MIN_SCORE
                and momentum_strength > epsilon
                and rs_percentile >= 80
                and ma20 > 0
                and close >= ma20
                and vol_avg20 > 0
                and vol >= vol_avg20
            )
            breakout_ok = breakout_context_ok
            pullback_ok = pullback_context_ok
            momentum_ok = momentum_context_ok
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

            breakout_strength = min(1.0, max(0.0, (close - high_50) / high_50 * 10.0)) if high_50 > 0 else 0.0
            pullback_strength = max(0.0, 1.0 - abs(pullback_pct - 0.10) / 0.05)
            momentum_strength = min(1.0, max(0.0, (rs_percentile - 80) / 20.0)) if rs_percentile >= 80 else 0.0
            breakout_ok = (
                breakout_strength > epsilon
                and (breakout_strength * 100.0) >= BREAKOUT_MIN_SCORE
                and high_50 > 0
                and vol_avg20 > 0
                and close >= high_50
                and vol > (vol_avg20 * 1.2)
            )
            pullback_ok = (
                pullback_strength > epsilon
                and (pullback_strength * 100.0) >= PULLBACK_MIN_SCORE
                and ma50 > 0
                and close >= ma50
                and 0.03 <= pullback_pct <= 0.18
                and vol_avg20 > 0
            )
            momentum_ok = (
                momentum_strength > epsilon
                and (momentum_strength * 100.0) >= MOMENTUM_MIN_SCORE
                and rs_percentile >= 80
                and ma20 > 0
                and vol_avg20 > 0
                and close >= ma20
                and vol >= vol_avg20
            )

        pass_total = int(breakout_ok) + int(pullback_ok) + int(momentum_ok)
        selected_style = str(
            item.get("entry_style_selected")
            or (precomputed_row or {}).get("entry_style_selected")
            or item.get("entry_signal")
            or ""
        ).strip().upper()
        selected_family = normalize_entry_setup_family(selected_style)
        family_setup_ok = {
            "ENTRY_BREAKOUT": breakout_strength * 100.0 >= BREAKOUT_MIN_SCORE,
            "ENTRY_PULLBACK": pullback_strength * 100.0 >= PULLBACK_MIN_SCORE,
            "ENTRY_MOMENTUM": momentum_strength * 100.0 >= MOMENTUM_MIN_SCORE,
        }.get(selected_family, pass_total > 0)
        family_trigger_ok = {
            "ENTRY_BREAKOUT": breakout_ok,
            "ENTRY_PULLBACK": pullback_ok,
            "ENTRY_MOMENTUM": momentum_ok,
        }.get(selected_family, pass_total > 0)
        evaluation = build_entry_evaluation(
            code=code,
            as_of=item.get("as_of") or (precomputed_row or {}).get("as_of") or "",
            trade_date=os.getenv("TRADE_DATE") or "",
            input_source="final30_locked" if has_precomputed else "watchlist_scan",
            setup_ok=family_setup_ok,
            score_ok=family_setup_ok,
            risk_ok=True,
            sizing_ok=True,
            buyable_ok=True,
            trigger_ok=family_trigger_ok,
            order_ready=bool(family_setup_ok and family_trigger_ok),
            reasons=reasons,
            setup_family=selected_style,
            decision_reason="ORDER_READY" if family_setup_ok and family_trigger_ok else (reasons[0] if reasons else "ENTRY_CONDITION_NOT_MET"),
            features=precomputed_row or item,
        )
        evaluations.append(evaluation)
        setup_ok_count += int(bool(evaluation.get("setup_ok")))
        breakout_pass_count += int(breakout_ok)
        pullback_pass_count += int(pullback_ok)
        momentum_pass_count += int(momentum_ok)
        multi_pass_count += int(pass_total >= 2)
        none_pass_count += int(pass_total == 0)

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

        signal_candidates: list[EntrySignal] = []
        if breakout_ok:
            signal = EntrySignal(
                code=code,
                name=name,
                strategy="breakout",
                close=close,
                signal_strength=breakout_strength,
                meta={"high_50": high_50, "volume": vol, "volume_avg20": vol_avg20, "evaluation": evaluation},
            )
            breakout.append(signal)
            signal_candidates.append(signal)
        if pullback_ok:
            signal = EntrySignal(
                code=code,
                name=name,
                strategy="pullback",
                close=close,
                signal_strength=pullback_strength,
                meta={"ma50": ma50, "high_52w": high_52w, "pullback_pct": pullback_pct * 100.0, "evaluation": evaluation},
            )
            pullback.append(signal)
            signal_candidates.append(signal)
        if momentum_ok:
            signal = EntrySignal(
                code=code,
                name=name,
                strategy="momentum",
                close=close,
                signal_strength=momentum_strength,
                meta={"rs_percentile": rs_percentile, "ma20": ma20, "volume": vol, "volume_avg20": vol_avg20, "evaluation": evaluation},
            )
            momentum.append(signal)
            signal_candidates.append(signal)

        if signal_candidates:
            signal = max(signal_candidates, key=lambda item: item.signal_strength)
            style_counts[signal.strategy] += 1
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
        "[ENTRY_SCAN][SUMMARY] total=%s passed=%s setup_ok=%s breakout_pass=%s pullback_pass=%s momentum_pass=%s multi_pass=%s none_pass=%s thresholds=%s source=%s rejected_counts=%s",
        total,
        passed,
        setup_ok_count,
        breakout_pass_count,
        pullback_pass_count,
        momentum_pass_count,
        multi_pass_count,
        none_pass_count,
        {
            "breakout": BREAKOUT_MIN_SCORE,
            "pullback": PULLBACK_MIN_SCORE,
            "momentum": MOMENTUM_MIN_SCORE,
        },
        {
            "precomputed": len(precomputed_map),
            "watchlist": total,
            "trade_precomputed_only": int(bool(trade_precomputed_only)),
        },
        dict(rejected_counts),
    )
    if total and breakout_pass_count == total and pullback_pass_count == total and momentum_pass_count == total:
        logger.warning("[ENTRY_SCAN][ANOMALY][ALL_PASS] total=%s", total)
    if total and multi_pass_count == total:
        logger.warning("[ENTRY_SCAN][ANOMALY][MULTI_PASS_EXCESS] total=%s", total)
    if total and none_pass_count == total:
        logger.warning("[ENTRY_SCAN][ANOMALY][NONE_PASS_EXCESS] total=%s", total)
    dominant_style = max(style_counts.values()) / total if total and style_counts else 0.0
    if dominant_style >= 0.8:
        logger.warning("[ENTRY_SCAN][STYLE_DISTRIBUTION_WARN] counts=%s total=%s", dict(style_counts), total)
        logger.warning("[ENTRY_SCAN][ANOMALY][STYLE_MONOCULTURE] counts=%s total=%s", dict(style_counts), total)

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
                "thresholds": {
                    "breakout": BREAKOUT_MIN_SCORE,
                    "pullback": PULLBACK_MIN_SCORE,
                    "momentum": MOMENTUM_MIN_SCORE,
                },
            },
            "per_symbol_rejection_reasons": per_symbol_rejects,
            "aggregate_rejected_counts": dict(rejected_counts),
            "candidate_counts_by_style": dict(style_counts),
            "summary": {
                "total": total,
                "passed": passed,
                "setup_ok_count": setup_ok_count,
                "breakout_pass": breakout_pass_count,
                "pullback_pass": pullback_pass_count,
                "momentum_pass": momentum_pass_count,
                "multi_pass": multi_pass_count,
                "none_pass": none_pass_count,
                "thresholds": {
                    "breakout": BREAKOUT_MIN_SCORE,
                    "pullback": PULLBACK_MIN_SCORE,
                    "momentum": MOMENTUM_MIN_SCORE,
                },
                "source": {
                    "precomputed": len(precomputed_map),
                    "watchlist": total,
                },
            },
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
    return {
        "breakout": breakout,
        "pullback": pullback,
        "momentum": momentum,
        "all": all_signals,
        "evaluations": evaluations,
        "summary": {
            "total": total,
            "passed": passed,
            "setup_ok_count": setup_ok_count,
            "breakout_pass": breakout_pass_count,
            "pullback_pass": pullback_pass_count,
            "momentum_pass": momentum_pass_count,
            "multi_pass": multi_pass_count,
            "none_pass": none_pass_count,
        },
    }
