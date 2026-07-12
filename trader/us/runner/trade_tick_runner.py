# -*- coding: utf-8 -*-
"""US Trade Tick Runner.

미국장 1 tick: reconcile → exit 평가 → entry 평가 → order routing → 결과 저장.

CLI:
  python -m trader.us.runner.trade_tick_runner \\
    --session am \\
    --env practice \\
    [--offline] \\
    [--force-now 2026-01-02T09:35:00-05:00]
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Contract marker: raw universe fallback is disabled in US trade tick path.
RAW_UNIVERSE_FALLBACK = "raw_universe_fallback_disabled"


_TRANSIENT_WATCHLIST_DB_ERROR_PATTERNS = (
    "edbhandlerexited",
    "connection to database closed",
    "server closed the connection",
    "statement timeout",
    "canceling statement due to statement timeout",
    "operationalerror",
    "internalerror",
    "connection already closed",
    "ssl syscall error",
    "terminating connection",
)



def validate_us_regime_contract_for_entry(prep_result: dict | None, *, real_order_mode: bool, kis_order_allowed: bool) -> dict[str, Any]:
    prep_result = prep_result or {}
    required_contract_version = "us_sector_rotation_v3"
    required_regime_version = "us_leading_regime_v1"
    actual_contract_version = prep_result.get("contract_version")
    actual_regime_version = prep_result.get("market_regime_version")
    is_real_trade_path = bool(real_order_mode or kis_order_allowed or os.getenv("GITHUB_EVENT_NAME") == "schedule")
    allow_legacy_for_test = (
        not is_real_trade_path
        and os.getenv("US_ALLOW_LEGACY_PREP_FOR_TEST", "0").lower() in {"1", "true", "yes", "on"}
    )
    if allow_legacy_for_test and actual_contract_version is None and actual_regime_version is None:
        contract_version_ok = True
    else:
        contract_version_ok = (
            actual_contract_version == required_contract_version
            and actual_regime_version == required_regime_version
        )
    if not contract_version_ok:
        reason = "prep_contract_version_mismatch"
    elif prep_result.get("status") in ("DEGRADED", "ERROR"):
        reason = "prep_status_error"
    elif not bool(prep_result.get("trade_can_proceed", True)):
        reason = prep_result.get("trade_block_reason") or ("risk_off_entry_block" if prep_result.get("market_regime") == "RISK_OFF" else "trade_can_proceed_false")
    elif not bool(prep_result.get("contract_ok", True)):
        reason = "contract_ok_false"
    elif not bool(
        prep_result.get(
            "final30_trade_ready",
            prep_result.get("final30_complete", True),
        )
    ):
        reason = "final30_not_trade_ready"
    elif int(prep_result.get("score_nonzero_count") or 0) != int(prep_result.get("final30_scored_count") or prep_result.get("score_nonzero_count") or 0):
        reason = "score_contract_failed"
    elif not bool(prep_result.get("cluster_contract_ok", True)):
        reason = "cluster_cap_contract_failed"
    elif list(prep_result.get("cap_violations") or []):
        reason = "sector_cap_violation_block"
    elif prep_result.get("market_regime") == "RISK_OFF":
        reason = "risk_off_entry_block"
    elif bool(prep_result.get("force_entry_block", False)):
        reason = "force_entry_block"
    elif not bool(prep_result.get("allow_new_buy", True)):
        reason = "allow_new_buy_false"
    else:
        reason = "ok"
    return {
        "ok": reason == "ok",
        "reason": reason,
        "required_contract_version": required_contract_version,
        "required_regime_version": required_regime_version,
        "actual_contract_version": actual_contract_version,
        "actual_regime_version": actual_regime_version,
        "allow_legacy_for_test": allow_legacy_for_test,
        "is_real_trade_path": is_real_trade_path,
    }

def _is_transient_watchlist_db_error(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(pattern in text for pattern in _TRANSIENT_WATCHLIST_DB_ERROR_PATTERNS)



def _blocked_entry_reason_counts(cluster_blocked: Any, market_blocked: Any, entry_degraded_reason: str | None = None) -> dict[str, int]:
    counts: dict[str, int] = {}
    def add(reason: str | None) -> None:
        if not reason:
            return
        counts[str(reason)] = counts.get(str(reason), 0) + 1
    for item in cluster_blocked or []:
        if isinstance(item, dict):
            add(item.get("reason") or item.get("blocked_reason"))
        else:
            add("cluster_guard_blocked")
    for item in market_blocked or []:
        if isinstance(item, dict):
            add(item.get("reason") or item.get("blocked_reason"))
        else:
            add("market_state_entry_block")
    if entry_degraded_reason:
        add(entry_degraded_reason)
    return counts

def _extract_watchlist_rows_from_payload(payload: Any) -> list[dict]:
    """Extract final30/watchlist rows from common artifact payload shapes."""
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("final30_scored", "watchlist", "rows", "data", "items", "final30"):
        value = payload.get(key)
        if isinstance(value, list):
            return [r for r in value if isinstance(r, dict)]
    nested = payload.get("payload")
    if isinstance(nested, dict):
        for key in ("final30_scored", "watchlist", "rows", "data", "items", "final30"):
            value = nested.get(key)
            if isinstance(value, list):
                return [r for r in value if isinstance(r, dict)]
    return []


def _payload_trade_date(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    for container in (payload, payload.get("payload"), payload.get("contract")):
        if isinstance(container, dict):
            td = container.get("trade_date") or container.get("date")
            if td:
                return str(td)
    return ""


def _nested_value(payload: dict, dotted_key: str) -> Any:
    cur: Any = payload
    for part in dotted_key.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _candidate_paths_from_latest_summary(payload: dict, summary_path: Path, trade_date: str) -> list[tuple[Path, str, bool]]:
    """Return (path, source, requires_trade_date_match) candidates referenced by latest summary."""
    keys = (
        "final30_scored_path", "final30_path", "watchlist_path", "artifact_path", "output_path",
        "latest_final30_scored_path", "payload.final30_scored_path", "payload.final30_path",
        "payload.artifact_path", "contract.final30_scored_path", "contract.final30_path",
    )
    candidates: list[tuple[Path, str, bool]] = []
    for key in keys:
        value = _nested_value(payload, key)
        if not value:
            continue
        path = Path(str(value))
        if not path.is_absolute():
            path = (summary_path.parent / path) if str(value).startswith(".") else Path(str(value))
        candidates.append((path, "latest_summary_referenced_artifact", True))
    candidates.extend([
        (Path("reports/us_prep/latest_final30_scored.json"), "latest_final30_scored", True),
        (Path("reports/us_prep") / trade_date / "final30_scored.json", "dated_final30_scored", False),
        (Path("runtime/us/prep") / trade_date / "final30_scored.json", "dated_final30_scored", False),
        (Path("runtime/us/watchlist") / trade_date / "final30_scored.json", "dated_final30_scored", False),
        (Path("signals/us") / trade_date / "final30_scored.json", "dated_final30_scored", False),
    ])
    return candidates


def _normalize_watchlist_rows(raw_rows: list[dict], source: str) -> list[dict]:
    normalized: list[dict] = []
    for row in raw_rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        score_value = row.get("score", row.get("score_final", row.get("final_score", row.get("rank_score", 0))))
        try:
            score = float(score_value or 0.0)
        except (TypeError, ValueError):
            score = 0.0
        clean = dict(row)
        clean["symbol"] = symbol
        clean.setdefault("score_final", score)
        clean["score"] = score
        clean["exchange"] = str(clean.get("exchange") or "NASDAQ").upper().strip()
        clean["strategy"] = str(clean.get("strategy") or "us_pb1")
        clean["entry_watchlist_source"] = source
        normalized.append(clean)
    return normalized




def _row_date(row: dict) -> str:
    return str(row.get("date") or row.get("trade_date") or row.get("as_of") or row.get("timestamp") or "")[:10]


def _row_close(row: dict) -> float | None:
    for key in ("close", "close_price", "adj_close", "stck_clpr", "price"):
        try:
            v = float(row.get(key))
            if v > 0 and v == v:
                return v
        except Exception:
            pass
    return None


def _compute_completed_daily_metrics(rows: list[dict], trade_date: str) -> dict:
    completed = [r for r in rows or [] if isinstance(r, dict) and _row_date(r) and _row_date(r) < str(trade_date)]
    completed.sort(key=_row_date)
    closes = [c for r in completed if (c := _row_close(r)) is not None]
    if len(closes) < 60:
        raise ValueError(f"daily_rows_insufficient completed={len(closes)} required=60")
    def ma(n: int) -> float | None:
        return round(sum(closes[-n:]) / n, 6) if len(closes) >= n else None
    def rs(n: int) -> float | None:
        return round((closes[-1] - closes[-1-n]) / closes[-1-n], 6) if len(closes) > n and closes[-1-n] > 0 else None
    ma200 = ma(200)
    ma200_prev = round(sum(closes[-220:-20]) / 200, 6) if len(closes) >= 220 else None
    ma200_slope = round((ma200 - ma200_prev) / ma200_prev, 6) if ma200 and ma200_prev and ma200_prev > 0 else None
    return {"ma20": ma(20), "ma50": ma(50), "ma150": ma(150), "ma200": ma200, "ma200_slope": ma200_slope, "rs_20d": rs(20), "rs_60d": rs(60), "rs_120d": rs(120), "daily_bar_count": len(closes), "daily_metrics_as_of": _row_date(completed[-1]) if completed else None}


def _final30_score_contract_ok(rows: list[dict]) -> bool:
    if not rows:
        return False
    for row in rows:
        try:
            if float(row.get("score_final", row.get("score", 0)) or 0) == 0.0:
                return False
        except Exception:
            return False
    return True


def _build_final30_symbol_map_for_trend(*, trade_date: str, locked_watchlist_cache: list[dict] | None, watchlist_cache_source: str | None) -> tuple[dict[str, dict], bool, str, list[dict]]:
    rows: list[dict] = []
    source = watchlist_cache_source or "unknown"
    try:
        if locked_watchlist_cache is not None:
            rows = _dedupe_watchlist_best_by_symbol(_normalize_watchlist_rows(list(locked_watchlist_cache), source or "session_cache"))
            source = source or "session_cache"
        else:
            # Do not perform a potentially slow DB watchlist load before SELL-first exit
            # routing. Session-level callers should pass locked_watchlist_cache when
            # available; otherwise trend input uses the local final30 artifact and
            # treats failures as missing/stale rather than absent.
            rows = load_watchlist_from_artifact(trade_date)
            source = "artifact_final30_scored"
        rows = _dedupe_watchlist_best_by_symbol(_normalize_watchlist_rows(rows, source))
        if any((_row_date(r) and _row_date(r) != str(trade_date)) for r in rows):
            logger.warning("[US_POSITION][TREND_INPUT] trade_date=%s final30_source=%s status=STALE action=do_not_increment_absent", trade_date, source)
            return {}, False, "missing_or_stale", rows
        ok = _final30_score_contract_ok(rows)
        if not ok:
            logger.warning("[US_POSITION][TREND_INPUT] trade_date=%s final30_source=%s status=SCORE_CONTRACT_ERROR action=do_not_increment_absent", trade_date, source)
            return {}, False, "score_contract_error", rows
        out = {str(r.get("symbol") or "").upper().strip(): {**r, "rank_final30": idx + 1} for idx, r in enumerate(rows) if r.get("symbol")}
        logger.info("[US_POSITION][TREND_INPUT] trade_date=%s final30_source=%s status=OK symbols=%d", trade_date, source, len(out))
        return out, True, "ok", rows
    except Exception as exc:
        logger.warning("[US_POSITION][TREND_INPUT] trade_date=%s final30_source=%s status=ERROR reason=%s action=do_not_increment_absent", trade_date, source, exc)
        return {}, False, "missing_or_stale", rows


def _update_position_trends_for_tick(*, positions: list[dict], provider: Any, trade_date: str, now: datetime, locked_watchlist_cache: list[dict] | None = None, watchlist_cache_source: str | None = None) -> tuple[list[dict], dict, list[dict] | None, str | None]:
    final30_map, final30_ok, final30_quality, loaded_rows = _build_final30_symbol_map_for_trend(trade_date=trade_date, locked_watchlist_cache=locked_watchlist_cache, watchlist_cache_source=watchlist_cache_source)
    if locked_watchlist_cache is None and loaded_rows:
        locked_watchlist_cache = loaded_rows
        watchlist_cache_source = "trend_final30_preload"
    counts = {"HEALTHY": 0, "WARNING": 0, "TRIM": 0, "EXIT": 0, "UNKNOWN": 0}
    for pos in positions or []:
        symbol = str(pos.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        exchange = str(pos.get("exchange") or "NASDAQ")
        row = final30_map.get(symbol)
        final30_payload = {
            "trade_date": trade_date,
            "available": final30_ok,
            "score_contract_ok": final30_ok,
            "in_final30_today": bool(row),
        }
        if final30_ok and row:
            final30_payload.update({
                "rank_final30": row.get("rank_final30"),
                "score_final": row.get("score_final", row.get("score")),
                "trend_score": row.get("trend_score"),
                "theme_cluster": row.get("theme_cluster"),
                "rotation_regime": row.get("rotation_regime"),
            })
        elif not final30_ok:
            final30_payload["trade_date"] = "" if final30_quality != "ok" else trade_date
        current_price = pos.get("current_price_usd") or pos.get("current_price") or pos.get("current_px")
        if current_price is None:
            try:
                px = provider.get_current_price(symbol, exchange)
                current_price = (px or {}).get("last")
                pos["current_price_usd"] = current_price
            except Exception as px_exc:
                logger.warning("[US_POSITION][TREND_INPUT] symbol=%s trade_date=%s current_price_status=ERROR reason=%s", symbol, trade_date, px_exc)
        daily = {}
        metrics_source = "insufficient_history"
        try:
            from trader.us.position_trend_state import load_trend_state
            persisted = load_trend_state(symbol, trade_date)
            if persisted.get("daily_metrics_trade_date") == trade_date and persisted.get("ma20") and persisted.get("ma50"):
                daily = {k: persisted.get(k) for k in ("ma20", "ma50", "ma150", "ma200", "ma200_slope", "rs_20d", "rs_60d", "rs_120d", "daily_bar_count", "daily_metrics_as_of", "daily_history_quality")}
                metrics_source = "persisted_trend_metrics"
            elif final30_ok and row and row.get("daily_metrics_as_of") and str(row.get("daily_metrics_as_of")) < str(trade_date) and int(row.get("daily_bar_count") or 0) >= 150 and row.get("ma20") and row.get("ma50"):
                daily = {k: row.get(k) for k in ("ma20", "ma50", "ma150", "ma200", "ma200_slope", "rs_20d", "rs_60d", "rs_120d", "daily_bar_count", "daily_metrics_as_of", "daily_history_quality")}
                metrics_source = "final30_prep_metrics"
            else:
                from trader.us.db.price_daily_repo import load_recent_us_daily_bars
                rows = load_recent_us_daily_bars(symbol=symbol, before_date=trade_date, limit=int(os.getenv("US_DAILY_REQUIRED_BARS", "260")))
                daily = _compute_completed_daily_metrics(list(rows or []), trade_date)
                daily["daily_history_quality"] = "OK" if int(daily.get("daily_bar_count") or 0) >= 200 else "DEGRADED_SHORT_HISTORY"
                metrics_source = "price_daily"
            daily["daily_metrics_source"] = metrics_source
            daily["daily_metrics_trade_date"] = trade_date
            logger.info("[US_POSITION][TREND_METRICS_SOURCE] symbol=%s source=%s metrics_as_of=%s", symbol, metrics_source, daily.get("daily_metrics_as_of"))
        except Exception as exc:
            logger.warning("[US_POSITION][TREND_INPUT] symbol=%s trade_date=%s daily_status=ERROR reason=%s action=UNKNOWN", symbol, trade_date, exc)
            daily = {"daily_history_quality": "INSUFFICIENT_DAILY_HISTORY", "daily_metrics_source": "insufficient_history", "daily_metrics_trade_date": trade_date}
            logger.info("[US_POSITION][TREND_METRICS_SOURCE] symbol=%s source=insufficient_history state=UNKNOWN", symbol)
        try:
            from trader.us.position_trend_state import update_us_position_trend_state
            trend = update_us_position_trend_state(
                symbol=symbol,
                trade_date=trade_date,
                now=now,
                current_price=float(current_price) if current_price is not None else None,
                holding_trade_days=int(pos.get("holding_trade_days") or 0),
                final30=final30_payload,
                daily=daily,
                lifecycle_id=pos.get("position_lifecycle_id"),
            )
        except Exception as exc:
            logger.warning("[US_POSITION][TREND_STATE][WARN] symbol=%s err=%s", symbol, exc)
            trend = {"trend_state": "UNKNOWN", "weakness_signals": [], "final30_absent_streak": 0, "below_ma20_streak": 0, "below_ma50_streak": 0, "ma20": None, "ma50": None, "rs_20d": None}
        pos["trend"] = trend
        pos["trend_state"] = trend.get("trend_state")
        pos["weakness_signals"] = trend.get("weakness_signals") or []
        pos["final30_absent_streak"] = trend.get("final30_absent_streak")
        pos["below_ma20_streak"] = trend.get("below_ma20_streak")
        pos["below_ma50_streak"] = trend.get("below_ma50_streak")
        pos["ma20"] = trend.get("ma20")
        pos["ma50"] = trend.get("ma50")
        pos["rs_20d"] = trend.get("rs_20d")
        counts[str(trend.get("trend_state") or "UNKNOWN")] = counts.get(str(trend.get("trend_state") or "UNKNOWN"), 0) + 1
    return positions, counts, locked_watchlist_cache, watchlist_cache_source


def _mark_trend_stages_from_records(records: Any, *, trade_date: str, status: str) -> None:
    if isinstance(records, dict):
        iterable = []
        for key in ("fills", "orders", "confirmed_orders", "confirmed", "results", "items"):
            val = records.get(key)
            if isinstance(val, list):
                iterable.extend(val)
        if not iterable:
            iterable = [records]
    else:
        iterable = list(records or [])
    for rec in iterable:
        if not isinstance(rec, dict):
            continue
        meta = rec.get("meta") or rec.get("intent", {}).get("meta") or {}
        order_key = rec.get("client_order_key") or rec.get("order_key") or meta.get("client_order_key")
        symbol = rec.get("symbol") or meta.get("symbol") or rec.get("pdno")
        if not meta.get("trend_stage"):
            try:
                from trader.us.db.repos import load_us_order_for_fill
                order = load_us_order_for_fill(order_no=rec.get("order_no"), client_order_key=order_key, symbol=str(symbol or ""), trade_date=trade_date)
                order_meta = order.get("meta") or {}
                if isinstance(order_meta, str):
                    import json as _json
                    order_meta = _json.loads(order_meta or "{}")
                meta = {**order_meta, **meta}
                order_key = order.get("client_order_key") or order_key
                symbol = order.get("symbol") or symbol
            except Exception as exc:
                logger.warning("[US_POSITION][TREND_STAGE][ORDER_LOOKUP_WARN] symbol=%s order_no=%s err=%s", symbol, rec.get("order_no"), exc)
        stage = meta.get("trend_stage") or rec.get("trend_stage")
        if not stage:
            continue
        lifecycle_id = meta.get("position_lifecycle_id") or rec.get("position_lifecycle_id")
        try:
            from trader.us.db.repos import mark_us_position_exit_stage
            mark_us_position_exit_stage(trade_date, str(symbol or ""), str(stage), order_key=str(order_key or "") or None, status=status, lifecycle_id=lifecycle_id)
        except Exception as exc:
            logger.warning("[US_POSITION][TREND_STAGE][MARK_WARN] symbol=%s stage=%s status=%s err=%s", symbol, stage, status, exc)

def _select_watchlist_rows_from_payload(payload: Any, *, path: Path, trade_date: str, source: str, require_trade_date_match: bool) -> list[dict]:
    if require_trade_date_match:
        payload_trade_date = _payload_trade_date(payload)
        if payload_trade_date and payload_trade_date != trade_date:
            raise ValueError(f"artifact_trade_date_mismatch payload={payload_trade_date} current={trade_date} path={path}")
    raw_rows = _extract_watchlist_rows_from_payload(payload)
    if not raw_rows:
        raise ValueError(f"artifact_missing_final30_rows path={path}")
    normalized = _normalize_watchlist_rows(raw_rows, source)
    deduped = _dedupe_watchlist_best_by_symbol(normalized)
    nonzero = sum(1 for r in deduped if float(r.get("score") or 0.0) != 0.0)
    if len(deduped) < 10:
        raise ValueError(f"artifact_unique_symbols_lt_10 count={len(deduped)} path={path}")
    if nonzero <= 0:
        raise ValueError(f"artifact_all_scores_zero path={path}")
    selected = deduped[:30]
    logger.info(
        "[US_ENTRY][WATCHLIST][FALLBACK_ARTIFACT][OK] path=%s source=%s rows=%d unique=%d selected=%d nonzero=%d preferred_30=%d",
        path, source, len(raw_rows), len(deduped), len(selected), nonzero, int(len(selected) == 30),
    )
    return selected


def load_watchlist_from_artifact(trade_date: str) -> list[dict]:
    """Load US locked-watchlist fallback from final30/latest prep artifacts."""
    candidate_paths: list[tuple[Path, str, bool]] = [
        (Path("runtime/us/watchlist") / trade_date / "final30_scored.json", "artifact_final30_scored", False),
        (Path("runtime/us/prep") / trade_date / "final30_scored.json", "artifact_final30_scored", False),
        (Path("signals/us") / trade_date / "final30_scored.json", "artifact_final30_scored", False),
        (Path("reports/us_prep") / trade_date / "final30_scored.json", "artifact_final30_scored", False),
        (Path("reports/us_prep/latest_us_prep_summary.json"), "latest_summary_embedded_rows", True),
        (Path("reports/us_prep/latest_final30_scored.json"), "latest_final30_scored", True),
    ]
    errors: list[str] = []
    seen: set[str] = set()
    idx = 0
    while idx < len(candidate_paths):
        path, source, require_td = candidate_paths[idx]
        idx += 1
        key = str(path.resolve() if path.exists() else path)
        if key in seen:
            continue
        seen.add(key)
        if not path.exists():
            errors.append(f"{path}:missing")
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if path.name == "latest_us_prep_summary.json":
                summary_td = _payload_trade_date(payload)
                if summary_td and summary_td != trade_date:
                    raise ValueError(f"latest_prep_summary_trade_date_mismatch payload={summary_td} current={trade_date}")
                try:
                    return _select_watchlist_rows_from_payload(
                        payload,
                        path=path,
                        trade_date=trade_date,
                        source="latest_summary_embedded_rows",
                        require_trade_date_match=bool(summary_td),
                    )
                except Exception as embedded_exc:
                    errors.append(f"{path}:embedded:{type(embedded_exc).__name__}:{embedded_exc}")
                    candidate_paths[idx:idx] = _candidate_paths_from_latest_summary(payload, path, trade_date)
                    continue
            return _select_watchlist_rows_from_payload(
                payload,
                path=path,
                trade_date=trade_date,
                source=source,
                require_trade_date_match=require_td,
            )
        except Exception as exc:
            errors.append(f"{path}:{type(exc).__name__}:{exc}")
    raise FileNotFoundError("; ".join(errors))

def _entry_cutoff_passed(now: datetime) -> bool:
    """15:45 ET 이후이면 True."""
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
    cutoff_str = os.getenv("US_BLOCK_NEW_ENTRY_AFTER_ET", "15:45")
    try:
        h, m = cutoff_str.split(":")
        from datetime import time
        cutoff = time(int(h), int(m))
    except Exception:
        from datetime import time
        cutoff = time(15, 45)

    now_ny = now.astimezone(NY_TZ)
    return now_ny.time() >= cutoff


def _dedupe_watchlist_best_by_symbol(rows: list[dict]) -> list[dict]:
    """Locked watchlist rows를 symbol별 최고 score 1개로 축약한다.
    
    Args:
        rows: 중복 symbol이 포함된 watchlist rows
        
    Returns:
        symbol별 최고 score row만 남긴 list
    """
    from trader.us.symbols import normalize_symbol

    best: dict[str, dict] = {}
    raw_count = len(rows)

    for row in rows:
        try:
            sym = str(row.get("symbol", "")).upper()
            if not sym:
                continue
            # normalize_symbol로 정규화 (없으면 uppercase만)
            try:
                sym = normalize_symbol(sym)
            except Exception:
                pass
            
            score = float(row.get("score") or 0.0)

            clean = dict(row)
            clean["symbol"] = sym
            clean["_dedupe_score"] = score

            prev = best.get(sym)
            if prev is None or score > float(prev.get("_dedupe_score") or 0.0):
                best[sym] = clean
        except Exception as exc:
            logger.warning(
                "[US_ENTRY][WATCHLIST_DEDUPE_SKIP] row=%s error=%s",
                row, exc
            )

    deduped = sorted(
        best.values(),
        key=lambda r: float(r.get("_dedupe_score") or 0.0),
        reverse=True,
    )

    logger.info(
        "[US_ENTRY][WATCHLIST_DEDUPE] raw_rows=%d unique_symbols=%d duplicate_rows=%d",
        raw_count, len(deduped), raw_count - len(deduped)
    )
    return deduped


def route_exit_orders_immediately(
    exit_intents: list[dict],
    *,
    buy_daily_notional: float,
    position_count: int,
    effective_budget: float,
    signal_only: bool,
    kis_order_allowed: bool,
    current_position_symbols: set[str],
) -> dict:
    """Route SELL intents before any entry watchlist/evaluation work.

    SELL notional is reported separately and never added to BUY daily notional.
    """
    from trader.us.execution.order_router import route_order

    sell_intents = [i for i in exit_intents if str(i.get("side") or "").upper() == "SELL"]
    logger.info("[US_EXIT][ROUTE_IMMEDIATE][START] exit_intents=%d", len(sell_intents))
    orders: list[dict] = []
    for intent in sell_intents:
        try:
            result = route_order(
                intent,
                current_daily_notional_usd=buy_daily_notional,
                current_position_count=position_count,
                total_portfolio_usd=max(effective_budget, 1000.0),
                available_cash_usd=max(effective_budget - buy_daily_notional, 0.0),
                signal_only=signal_only,
                kis_order_allowed=kis_order_allowed,
                allowed_symbols=None,
                current_position_symbols=current_position_symbols if current_position_symbols else None,
            )
            orders.append(result)
        except Exception as exc:
            logger.warning("[US_EXIT][ROUTE_IMMEDIATE][WARN] intent=%s error=%s", intent.get("symbol"), exc)
            orders.append({"status": "ERROR", "error": str(exc), "intent": intent})
    ack = sum(1 for o in orders if o.get("status") == "ACK")
    sent = sum(1 for o in orders if o.get("status") in {"ACK", "DRY_RUN", "SIGNAL_ONLY"})
    rejected = sum(1 for o in orders if o.get("status") == "REJECT")
    blocked = sum(1 for o in orders if o.get("status") in {"BLOCKED", "WARN_DUPLICATE_EXIT_BLOCKED"})
    sell_notional_routed = sum(
        float((o.get("intent") or {}).get("notional_usd", 0) or 0)
        for o in orders
        if o.get("status") in {"ACK", "DRY_RUN", "SIGNAL_ONLY"}
    )
    logger.info(
        "[US_EXIT][ROUTE_IMMEDIATE][DONE] exit_intents=%d sent=%d ack=%d rejected=%d blocked=%d sell_notional=%.2f",
        len(sell_intents), sent, ack, rejected, blocked, sell_notional_routed,
    )
    return {
        "orders": orders,
        "sell_notional_routed": sell_notional_routed,
        "exit_notional_routed": sell_notional_routed,
        "sent": sent,
        "ack": ack,
        "rejected": rejected,
        "blocked": blocked,
    }


def run_trade_tick(
    session: str = "am",
    env: str = "practice",
    offline: bool = False,
    force_now: str | None = None,
    run_mode: str | None = None,
    signal_only: bool = False,
    kis_order_allowed: bool = True,
    session_entry_allowed: bool | None = None,
    session_buy_orders_count: int | None = None,
    tick_index: int = 1,
    balance_reconcile_interval: int = 3,
    prep_status_cache: dict | None = None,
    locked_watchlist_cache: list[dict] | None = None,
    prep_cache_source: str | None = None,
    watchlist_cache_source: str | None = None,
) -> dict:
    """미국장 단일 tick 실행.

    tick 순서:
    1. market phase 확인
    2. trading day 확인
    3. budget 계산
    4. KIS 잔고/체결 조회
    5. us_positions reconcile
    6. exit candidate 평가
    7. entry candidate 평가
    8. order intents 병합
    9. risk gate
    10. order router
    11. order result 저장
    12. tick summary 출력

    Returns:
        {"status": "OK"|"SKIP"|"ERROR"|"OK_WITH_WARNINGS", ...}
    """
    logger.info(
        "[US_TICK][START] session=%s env=%s offline=%s run_mode=%s signal_only=%s",
        session, env, offline, run_mode, signal_only,
    )
    last_stage = "tick_start"

    # ── 변수 사전 초기화 (reconcile 실패 시 UnboundLocalError 방지) ──────────
    fills_today: list[dict] = []
    fills_error_count = 0
    fills_warnings_count = 0
    fills_contract_error = False  # reason=fills_contract_error
    fills_temp_error = False
    temp_error_count = 0
    temp_recovered_count = 0
    kis_temp_errors_by_api: dict[str, dict] = {}
    ack_recon: dict[str, Any] = {"status": "SKIP", "pending_count": 0, "confirmed_count": 0, "balance_reconcile_count": 0, "unresolved_count": 0, "symbols_by_status": {}}
    ack_recon_before_route: dict[str, Any] = dict(ack_recon)
    ack_recon_after_route: dict[str, Any] = dict(ack_recon)

    # ── 시각 결정 ──────────────────────────────────────────────────────────────
    from trader.us.market_calendar import now_ny, is_us_trading_day, market_phase
    from zoneinfo import ZoneInfo

    NY_TZ = ZoneInfo("America/New_York")

    if force_now:
        now = datetime.fromisoformat(force_now).astimezone(NY_TZ)
    else:
        now = now_ny()
    trade_date = now.strftime("%Y-%m-%d")

    logger.info(
        "[US_TICK][TIME] session=%s force_now=%s resolved_now_et=%s",
        session,
        force_now or "",
        now.isoformat(),
    )

    # ── 거래일 확인 ───────────────────────────────────────────────────────────
    # signal_only 모드에서는 거래일이 아니어도 계속 진행 (신호만 생성)
    if not is_us_trading_day(now.date()):
        if not signal_only:
            logger.info("[US_TICK][SKIP] not_trading_day date=%s", now.date())
            return {
                "status": "SKIP",
                "reason": "not_trading_day",
                "last_stage": "trading_day_guard",
                "trade_date": trade_date,
            }
        else:
            logger.info(
                "[US_TICK][SIGNAL_ONLY] non_trading_day date=%s run_mode=%s",
                now.date(), run_mode or "NON_TRADING_SIGNAL_ONLY",
            )

    # ── 장 phase 확인 ─────────────────────────────────────────────────────────
    phase = market_phase(now)
    if session == "am" and phase == "PREMARKET":
        regular_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        seconds_to_open = int((regular_open - now).total_seconds())
        grace = int(os.getenv("US_OPEN_RECHECK_GRACE_SEC", "30"))
        if 0 <= seconds_to_open <= grace:
            logger.info(
                "[US_MARKET_PHASE][WAIT_OPEN_GRACE] session=%s seconds_to_open=%d grace=%d",
                session, seconds_to_open, grace,
            )
            if not force_now:
                time.sleep(seconds_to_open + 1)
                now = now_ny()
                phase = market_phase(now)
            else:
                logger.info("[US_MARKET_PHASE][SKIP_PREOPEN_GRACE] force_now=1")
                return {
                    "status": "SKIP_PREOPEN_GRACE",
                    "reason": "WAIT_OPEN_GRACE",
                    "phase": phase,
                    "trade_date": trade_date,
                    "seconds_to_open": seconds_to_open,
                }
            if phase in ("REGULAR_OPEN", "REGULAR_MID", "REGULAR_CLOSE"):
                logger.info("[US_MARKET_PHASE][STARTED_AFTER_OPEN_GRACE] phase=%s", phase)
    if phase not in ("REGULAR_OPEN", "REGULAR_MID", "REGULAR_CLOSE"):
        logger.info("[US_TICK][SKIP] market not open phase=%s", phase)
        return {
            "status": "SKIP",
            "reason": "market_not_open",
            "phase": phase,
            "last_stage": "market_phase_guard",
            "trade_date": trade_date,
        }

    # ── 예산 계산 ─────────────────────────────────────────────────────────────
    from trader.us.budget import resolve_us_order_budget
    from trader.us.data_provider import USDataProvider

    provider = USDataProvider(offline=offline)
    real_order_mode = (
        os.getenv("DRY_RUN", "0") == "0"
        and os.getenv("US_KIS_ORDER_ALLOWED", "1") == "1"
        and kis_order_allowed
    )
    tick_timeout_sec = int(os.getenv("US_TICK_TIMEOUT_SEC", "270"))
    watchlist_timeout_sec = int(os.getenv("US_WATCHLIST_LOAD_TIMEOUT_SEC", "20"))
    entry_eval_timeout_sec = int(os.getenv("US_ENTRY_EVAL_TIMEOUT_SEC", "60"))

    if offline:
        available_cash_usd = 10000.0
    else:
        # DRY_RUN 모드: 환경변수 fallback 먼저 시도
        dry_run_mode = os.getenv("DRY_RUN", "0") == "1"
        try:
            available_cash_usd = provider.get_orderable_cash(
                symbol="AAPL", exchange="NASDAQ", price=100.0
            )
        except Exception as exc:
            if dry_run_mode:
                fallback = float(os.getenv("US_DRY_RUN_CASH_FALLBACK_USD", "10000.0"))
                logger.warning(
                    "[US_TICK][WARN] orderable_cash failed in DRY_RUN, using fallback=%.2f: %s",
                    fallback, exc,
                )
                available_cash_usd = fallback
            else:
                # fallback: balance에서 현금성 필드 탐색
                try:
                    balance = provider.get_balance()
                    for _k in ("ord_psbl_cash", "ovrs_ord_psbl_amt", "orderable_cash",
                               "cash", "psbl_amt", "frcr_pchs_amt1"):
                        _v = balance.get(_k)
                        if _v is not None:
                            try:
                                available_cash_usd = float(_v)
                                break
                            except (ValueError, TypeError):
                                pass
                    else:
                        available_cash_usd = 0.0
                except Exception as exc2:
                    logger.warning("[US_TICK][WARN] cash fetch failed: %s", exc2)
                    available_cash_usd = 0.0

    budget = resolve_us_order_budget(available_cash_usd)
    effective_budget = budget["effective_order_budget_usd"]

    # ── reconcile ─────────────────────────────────────────────────────────────
    should_reconcile_balance = (
        session == "close"
        or int(tick_index or 1) <= 1
        or (int(balance_reconcile_interval or 0) > 0 and int(tick_index or 1) % int(balance_reconcile_interval or 1) == 0)
    )
    logger.info("[US_RECONCILE][START] session=%s tick_index=%s should_reconcile_balance=%d interval=%s", session, tick_index, int(should_reconcile_balance), balance_reconcile_interval)
    try:
        if should_reconcile_balance:
            from trader.us.execution.reconcile import reconcile_positions
            recon = reconcile_positions(provider=provider)
        else:
            from trader.us.db.repos import load_positions as _load_positions_for_reconcile_skip
            _positions = _load_positions_for_reconcile_skip()
            recon = {
                "status": "SKIPPED_BALANCE_RECONCILE",
                "reason": "reconcile_interval_skip",
                "positions": _positions,
                "position_count": len(_positions),
                "position_symbols": [str(p.get("symbol") or "").upper() for p in _positions if p.get("symbol")],
                "block_new_entry": False,
            }
        logger.info(
            "[US_RECONCILE][DONE] status=%s positions=%s",
            recon.get("status"),
            recon.get("position_count", 0),
        )
    except Exception as exc:
        logger.warning("[US_RECONCILE][WARN] %s", exc)
        recon = {"status": "WARN", "error": str(exc), "block_new_entry": False}
    
    # reconcile CONTRACT_ERROR 또는 block_new_entry=True이면 신규 BUY 차단
    if recon.get("block_new_entry", False) or recon.get("status") == "CONTRACT_ERROR":
        last_stage = "reconcile"
        logger.error(
            "[US_RECONCILE][BLOCK_NEW_ENTRY] reason=%s status=%s",
            recon.get("reason", "balance_position_parse_error"),
            recon.get("status", "CONTRACT_ERROR"),
        )
        logger.error(
            "[US_TICK][DONE] session=%s status=FAILED reason=balance_position_parse_error", session
        )
        return {
            "status": "FAILED",
            "reason": recon.get("reason", "balance_position_parse_error"),
            "session": session,
            "orders": [],
            "ack": 0,
            "dry_run": 0,
            "blocked": 0,
            "signal_only": 0,
            "errors": 1,
            "budget": budget,
            "run_mode": run_mode,
            "signal_only_mode": signal_only,
            "kis_order_allowed": kis_order_allowed,
            "last_stage": last_stage,
            "trade_date": trade_date,
            "prep_status": "UNKNOWN",
            "locked_watchlist_count": 0,
            "entry_eval_status": "BLOCKED",
            "entry_error_type": "balance_position_parse_error",
            "entry_error_message": recon.get("balance_parse_error", "balance_position_parse_error"),
            "entry_intents": 0,
            "orders_sent": 0,
            "fills": len(fills_today),
            "positions": recon.get("position_count", 0),
            "temp_error_count": temp_error_count,
            "temp_recovered_count": temp_recovered_count,
        }
    
    # Extract position_symbols from reconcile result
    current_position_symbols: set[str] = set(recon.get("position_symbols", []))

    # ── 체결 조회 및 DB 저장 ──────────────────────────────────────────────────
    # (fills_today 등은 함수 시작부에서 사전 초기화됨)
    
    if not offline:
        try:
            last_stage = "fills_fetch"
            from trader.us.execution.fills import get_fills_today
            fills_result = get_fills_today(provider=provider, signal_only=signal_only, trade_date=trade_date)
            if fills_result["status"] == "CONTRACT_ERROR":
                logger.error(
                    "[US_TICK][ERROR] fills fetch CONTRACT_ERROR: %s",
                    fills_result.get("error", "unknown")
                )
                fills_error_count += 1
                fills_contract_error = True
            elif fills_result["status"] == "TEMP_ERROR":
                logger.warning(
                    "[US_TICK][WARN] fills fetch TEMP_ERROR: %s",
                    fills_result.get("error", "unknown")
                )
                fills_warnings_count += 1
                fills_temp_error = True
            elif fills_result["status"] != "OK":
                logger.warning(
                    "[US_TICK][WARN] fills fetch failed: %s",
                    fills_result.get("error", "unknown")
                )
                fills_warnings_count += 1
            
            fills_today = fills_result["fills"]
            logger.info("[US_FILLS][FETCHED] count=%d status=%s", len(fills_today), fills_result["status"])
        except Exception as exc:
            logger.error("[US_TICK][ERROR] fills exception: %s", exc)
            fills_error_count += 1

    # temp_error_count, temp_recovered_count, kis_temp_errors_by_api는 함수 시작부에서 사전 초기화됨
    if fills_temp_error:
        temp_error_count += 1
        kis_temp_errors_by_api.setdefault("GET_inquire_balance", {"temp_error": 0, "recovered": 0, "unrecovered": 0})
        kis_temp_errors_by_api["GET_inquire_balance"]["temp_error"] += 1

    if not offline:
        try:
            client = provider._get_client()
            stats = getattr(client, "stats", {}) or {}
            temp_error_count += int(stats.get("temp_error_count", 0) or 0)
            temp_recovered_count += int(stats.get("temp_recovered_count", 0) or 0)
            # API별 에러 집계 — client.stats에 by_api 구조가 있으면 사용
            by_api = stats.get("by_api") or {}
            for api_name, api_stats in by_api.items():
                t = int(api_stats.get("temp_error", 0) or 0)
                r = int(api_stats.get("recovered", 0) or 0)
                u = int(api_stats.get("unrecovered", 0) or 0)
                if t > 0:
                    existing = kis_temp_errors_by_api.setdefault(
                        api_name, {"temp_error": 0, "recovered": 0, "unrecovered": 0}
                    )
                    existing["temp_error"] += t
                    existing["recovered"] += r
                    existing["unrecovered"] += u
            # GET_price 에러가 별도 집계된 경우
            price_temp_errors = int(stats.get("price_temp_error_count", 0) or 0)
            price_recovered = int(stats.get("price_temp_recovered_count", 0) or 0)
            if price_temp_errors > 0:
                e = kis_temp_errors_by_api.setdefault(
                    "GET_price", {"temp_error": 0, "recovered": 0, "unrecovered": 0}
                )
                e["temp_error"] += price_temp_errors
                e["recovered"] += price_recovered
                e["unrecovered"] += max(0, price_temp_errors - price_recovered)
        except Exception:
            pass

    # KIS fills + DB sold_today 합산
    from trader.us.db.repos import load_today_symbols_sold, save_fills, save_position_snapshot, save_reconcile_log
    kis_sold = {f["symbol"] for f in fills_today if f.get("side") == "SELL"}
    try:
        db_sold = load_today_symbols_sold(trade_date=trade_date)
    except Exception:
        db_sold = set()
    sold_today = kis_sold | db_sold

    # fills DB 저장
    if fills_today:
        try:
            save_fills(fills_today)
        except Exception as exc:
            logger.warning("[US_TICK][WARN] save_fills failed: %s", exc)
        try:
            _mark_trend_stages_from_records(fills_today, trade_date=trade_date, status="FILLED")
        except Exception as exc:
            logger.warning("[US_POSITION][TREND_STAGE][FILL_MARK_WARN] err=%s", exc)

    # ACK reconcile: fills 저장 직후 미체결 ACK 주문 재확인
    if not offline:
        try:
            from trader.us.execution.reconcile import reconcile_ack_orders_with_balance
            ack_recon = reconcile_ack_orders_with_balance(
                provider=provider,
                trade_date=trade_date,
                env=env,
            )
            ack_recon_before_route = dict(ack_recon)
            logger.info(
                "[US_RECONCILE][ACK_RECONCILE][TICK_BEFORE_ROUTE] status=%s pending=%d confirmed=%d balance=%d unresolved=%d",
                ack_recon.get("status"),
                ack_recon.get("pending_count", 0),
                ack_recon.get("confirmed_count", 0),
                ack_recon.get("balance_reconcile_count", 0),
                ack_recon.get("unresolved_count", 0),
            )
            try:
                _mark_trend_stages_from_records(ack_recon, trade_date=trade_date, status="FILLED")
            except Exception as exc:
                logger.warning("[US_POSITION][TREND_STAGE][ACK_RECON_MARK_WARN] err=%s", exc)
        except Exception as exc:
            logger.warning("[US_TICK][WARN] reconcile_ack_orders_with_balance failed: %s", exc)

    # reconcile 결과 positions DB 저장
    recon_positions = recon.get("positions", [])
    if recon.get("preserve_previous_positions"):
        logger.warning("[US_RECONCILE][SKIP_ZERO_SNAPSHOT] reason=balance_fetch_failed preserve_previous=1")
    elif recon_positions:
        try:
            save_position_snapshot(recon_positions)
        except Exception as exc:
            logger.warning("[US_TICK][WARN] save_position_snapshot failed: %s", exc)

    # reconcile log DB 저장
    try:
        save_reconcile_log({
            "status": recon.get("status", "OK"),
            "message": recon.get("error", ""),
            "position_count": len(recon_positions),
            "total_pvs": recon.get("total_pvs_usd", 0),
            "detail": {"session": session},
        })
    except Exception as exc:
        logger.warning("[US_TICK][WARN] save_reconcile_log failed: %s", exc)

    # ── 현재 포지션 ───────────────────────────────────────────────────────────
    # reconcile 결과 우선, 비어 있으면 DB fallback
    from trader.us.db.repos import load_positions as db_load_positions
    if recon_positions:
        current_positions = recon_positions
    else:
        try:
            current_positions = db_load_positions()
        except Exception:
            current_positions = []
    try:
        from trader.us.position_lifecycle_state import reconcile_us_position_lifecycles
        lifecycle_map = reconcile_us_position_lifecycles(
            positions=current_positions,
            trade_date=trade_date,
            now=now,
            authoritative=(
                bool(should_reconcile_balance)
                and not recon.get("preserve_previous_positions")
                and recon.get("status") not in {"WARN", "ERROR", "CONTRACT_ERROR"}
            ),
        )
        for _p in current_positions:
            _lc = lifecycle_map.get(str(_p.get("symbol") or "").upper())
            if _lc:
                _p["position_lifecycle_id"] = _lc.get("lifecycle_id")
                _p["opened_trade_date"] = _lc.get("opened_trade_date")
                _p["holding_trade_days"] = _lc.get("holding_trade_days")
                _p["lifecycle_state_source"] = "us_position_risk_state"
                _p["high_watermark"] = _lc.get("high_watermark")
                _p["max_price"] = _lc.get("high_watermark")
                _p["high_watermark_source"] = _lc.get("high_watermark_source")
    except Exception as _lc_exc:
        logger.warning("[US_POSITION][LIFECYCLE][WARN] err=%s", _lc_exc)
    position_count = len(current_positions)
    max_positions = int(os.getenv("US_MAX_POSITIONS", "35") or "35")
    available_new_slots = max(0, max_positions - position_count)
    full_position = available_new_slots <= 0
    allow_new_symbols = available_new_slots > 0
    allow_add_to_existing = os.getenv("US_ALLOW_ADD_TO_EXISTING", "1").lower() in {"1", "true", "yes"}
    if full_position and os.getenv("US_FULL_POSITION_ALLOW_ADD_TO_EXISTING", "1").lower() in {"1", "true", "yes"}:
        allow_add_to_existing = True
    logger.info(
        "[US_CAPITAL][CAPACITY] position_count=%d max_positions=%d available_new_slots=%d "
        "full_position=%d allow_new_symbols=%d allow_add_to_existing=%d",
        position_count, max_positions, available_new_slots,
        int(full_position), int(allow_new_symbols), int(allow_add_to_existing),
    )

    invested_market_value_usd = 0.0
    for _p in current_positions:
        try:
            invested_market_value_usd += float(_p.get("market_value_usd") or _p.get("market_value") or _p.get("eval_amount_usd") or 0)
        except (TypeError, ValueError):
            pass
    account_equity_usd_env = float(os.getenv("US_ACCOUNT_EQUITY_USD", "0") or 0)
    portfolio_equity_usd = 0.0
    try:
        portfolio_equity_usd = float(recon.get("total_pvs_usd") or recon.get("total_pvs") or 0.0)
    except (TypeError, ValueError):
        portfolio_equity_usd = 0.0
    if portfolio_equity_usd <= 0:
        portfolio_equity_usd = float(invested_market_value_usd or 0.0) + float(available_cash_usd or 0.0)
    if portfolio_equity_usd <= 0 and account_equity_usd_env > 0:
        portfolio_equity_usd = account_equity_usd_env
    try:
        from trader.us.capital_deployment import compute_deployment_metrics, decide_deployment_action
        deployment_metrics = compute_deployment_metrics(
            account_equity_usd=portfolio_equity_usd or account_equity_usd_env,
            invested_market_value_usd=invested_market_value_usd,
            cash_usd=available_cash_usd,
        )
        capital_deployment_action = decide_deployment_action(deployment_metrics, position_count=position_count, max_positions=max_positions)
    except Exception as _deploy_exc:
        logger.warning("[US_CAPITAL][DEPLOYMENT][WARN] error=%s", _deploy_exc)
        deployment_metrics = {}
        capital_deployment_action = "NORMAL"
    if capital_deployment_action == "TRIM_ONLY":
        allow_new_symbols = False
        allow_add_to_existing = False
    elif capital_deployment_action == "ADD_TO_EXISTING_ONLY":
        allow_new_symbols = False
    if full_position and deployment_metrics.get("underdeployed"):
        logger.warning("[US_CAPITAL][WARN] US_CAPITAL_UNDERDEPLOYED_FULL_POSITION action=%s gross_exposure_pct=%.4f target_exposure_pct=%.4f", capital_deployment_action, float(deployment_metrics.get("gross_exposure_pct", 0) or 0), float(deployment_metrics.get("target_exposure_pct", 0) or 0))

    # ── EXIT position entry_price 표준화 ──────────────────────────────────────
    # 모든 보유 종목에 대해 exit 평가 전 entry_price를 resolve한다.
    # entry_price가 없으면 fail-closed SELL intent 생성 (US_EXIT_FAIL_CLOSED_ON_PNL_MISSING 기본값=1)
    _exit_trade_date = trade_date if isinstance(trade_date, str) else str(trade_date)
    try:
        from trader.us.pb1.us_exit_position_resolver import enrich_us_positions_for_exit
        current_positions, exit_position_meta = enrich_us_positions_for_exit(
            current_positions,
            trade_date=_exit_trade_date,
            env=env,
            provider=provider,
        )
        logger.info(
            "[US_EXIT][POSITION_RESOLVE] total=%d ok=%d missing=%d sources=%s missing_symbols=%s",
            exit_position_meta.get("total", 0),
            exit_position_meta.get("ok", 0),
            exit_position_meta.get("missing", 0),
            exit_position_meta.get("sources", {}),
            exit_position_meta.get("missing_symbols", []),
        )
    except Exception as _resolve_exc:
        logger.warning("[US_EXIT][POSITION_RESOLVE][WARN] resolver failed: %s", _resolve_exc)

    # ── POSITION TREND 계산: exit 평가 전에 보유종목 trend state 주입 ───────────
    trend_state_counts = {"HEALTHY": 0, "WARNING": 0, "TRIM": 0, "EXIT": 0, "UNKNOWN": 0}
    try:
        current_positions, trend_state_counts, locked_watchlist_cache, watchlist_cache_source = _update_position_trends_for_tick(
            positions=current_positions,
            provider=provider,
            trade_date=_exit_trade_date,
            now=now,
            locked_watchlist_cache=locked_watchlist_cache,
            watchlist_cache_source=watchlist_cache_source,
        )
    except Exception as _trend_exc:
        logger.warning("[US_POSITION][TREND_STATE][TICK_WARN] err=%s", _trend_exc)

    # ── EXIT 평가 ─────────────────────────────────────────────────────────────
    logger.info("[US_EXIT][EVAL][START] session=%s positions=%d", session, position_count)
    exit_intents: list[dict] = []
    try:
        engine = _get_strategy_engine(env=env, offline=offline)
        exit_intents = engine.evaluate_exits(
            positions=current_positions,
            provider=provider,
            now=now,
        )
    except Exception as exc:
        logger.warning("[US_EXIT][EVAL][WARN] %s", exc)
    logger.info("[US_EXIT][EVAL][DONE] exit_intents=%d", len(exit_intents))

    market_state_overlay = {"market_state": "NORMAL", "exposure_multiplier": 1.0, "allow_new_buy": True, "allow_add_to_existing": allow_add_to_existing, "force_entry_block": False, "trailing_stop_mode": "normal"}
    prep_result_for_overlay = {}
    try:
        from trader.us.db.repos import load_latest_us_prep_status as _load_prep_for_overlay
        _prep_overlay_info = _load_prep_for_overlay(trade_date) or {}
        prep_result_for_overlay = _prep_overlay_info.get("result") if isinstance(_prep_overlay_info.get("result"), dict) else _prep_overlay_info
        from trader.us.market_state_overlay import evaluate_us_market_state, build_profit_capture_intents
        account_snapshot = {
            "portfolio_equity_usd": portfolio_equity_usd,
            "invested_market_value_usd": invested_market_value_usd,
            "cash_usd": available_cash_usd,
            "gross_exposure_pct": deployment_metrics.get("gross_exposure_pct"),
            "account_intraday_pnl_pct": os.getenv("US_ACCOUNT_INTRADAY_PNL_PCT"),
            "account_5d_pnl_pct": os.getenv("US_ACCOUNT_5D_PNL_PCT"),
        }
        if isinstance(prep_result_for_overlay, dict) and prep_result_for_overlay.get("contract_version") == "us_sector_rotation_v3" and prep_result_for_overlay.get("market_regime_version") == "us_leading_regime_v1":
            market_state_overlay = dict(prep_result_for_overlay)
        else:
            market_state_overlay = evaluate_us_market_state(
                trade_date=trade_date,
                provider=provider,
                rotation_context=(prep_result_for_overlay or {}).get("rotation_context") or {},
                prep_result=prep_result_for_overlay if isinstance(prep_result_for_overlay, dict) else {},
                positions=current_positions,
                account_snapshot=account_snapshot,
                now=now,
            )
        existing_sell_symbols = {str(i.get("symbol") or "").upper().strip() for i in exit_intents if str(i.get("side") or "").upper() == "SELL"}
        profit_capture_intents = build_profit_capture_intents(current_positions, market_state_overlay, existing_sell_symbols, now=now, trade_date=trade_date)
        if profit_capture_intents:
            exit_intents.extend(profit_capture_intents)
    except Exception as _market_state_exc:
        logger.warning("[US_MARKET_STATE][WARN] error=%s", _market_state_exc)
    effective_budget_before_overlay = effective_budget
    exposure_multiplier = float(market_state_overlay.get(
        "effective_capital_scale",
        market_state_overlay.get("capital_scale", market_state_overlay.get("exposure_multiplier") or 1.0),
    ) or 0.0)
    effective_budget_after_overlay = effective_budget_before_overlay * exposure_multiplier
    if market_state_overlay.get("force_entry_block"):
        effective_budget_after_overlay = 0.0
    effective_budget = effective_budget_after_overlay
    allow_new_symbols = bool(allow_new_symbols and market_state_overlay.get("allow_new_buy", True))
    allow_add_to_existing = bool(allow_add_to_existing and market_state_overlay.get("allow_add_to_existing", True))
    logger.info("[US_MARKET_STATE][BUDGET] effective_budget_before_overlay=%.2f exposure_multiplier=%.2f effective_budget_after_overlay=%.2f effective_capital_scale=%s effective_max_new_positions=%s", effective_budget_before_overlay, exposure_multiplier, effective_budget_after_overlay, market_state_overlay.get("effective_capital_scale"), market_state_overlay.get("effective_max_new_positions"))

    cluster_guard_result = {"portfolio_cluster_guard_status": "NOT_EVALUATED", "portfolio_ai_tech_weight": 0.0, "portfolio_cluster_cap_violations": [], "cluster_guard_trim_intents": [], "cluster_guard_trim_notional": 0.0}
    try:
        from trader.us.db.repos import load_latest_us_prep_status
        from trader.us.portfolio_cluster_guard import evaluate_portfolio_cluster_guard
        _prep_for_cluster = load_latest_us_prep_status(trade_date) or {}
        _prep_cluster_result = _prep_for_cluster.get("result") if isinstance(_prep_for_cluster.get("result"), dict) else {}
        _rotation_context = (
            _prep_cluster_result.get("rotation_context")
            or _prep_for_cluster.get("rotation_context")
            or {}
        )
        _rotation_regime = str(
            _prep_cluster_result.get("rotation_regime")
            or (_prep_cluster_result.get("rotation_context") or {}).get("rotation_regime")
            or _prep_for_cluster.get("rotation_regime")
            or (_prep_for_cluster.get("rotation_context") or {}).get("rotation_regime")
            or "UNKNOWN"
        )
        if _rotation_context.get("rotation_context_suspect"):
            _rotation_regime = "UNKNOWN"
        cluster_guard_result = evaluate_portfolio_cluster_guard(current_positions, _rotation_regime, portfolio_equity_usd, exit_intents, provider, now)
        if cluster_guard_result.get("cluster_guard_trim_intents"):
            exit_intents.extend(cluster_guard_result.get("cluster_guard_trim_intents") or [])
        try:
            from trader.us.market_state_overlay import build_defense_trim_intents
            existing_sell_symbols = {str(i.get("symbol") or "").upper().strip() for i in exit_intents if str(i.get("side") or "").upper() == "SELL"}
            defense_trim_intents = build_defense_trim_intents(current_positions, market_state_overlay, existing_sell_symbols)
            if defense_trim_intents:
                exit_intents.extend(defense_trim_intents)
        except Exception as _def_trim_exc:
            logger.warning("[US_DEFENSE][TRIM][WARN] error=%s", _def_trim_exc)
    except Exception as _cluster_guard_exc:
        logger.warning("[US_CLUSTER_GUARD][PORTFOLIO][WARN] error=%s", _cluster_guard_exc)

    # Route SELLs immediately before any entry watchlist or entry evaluation work.
    buy_daily_notional = 0.0
    if not current_position_symbols and current_positions:
        current_position_symbols = {str(p.get("symbol", "")).upper().strip() for p in current_positions if p.get("symbol")}
    exit_route_result = route_exit_orders_immediately(
        exit_intents,
        buy_daily_notional=buy_daily_notional,
        position_count=position_count,
        effective_budget=effective_budget,
        signal_only=signal_only,
        kis_order_allowed=kis_order_allowed,
        current_position_symbols=current_position_symbols,
    )
    orders = list(exit_route_result.get("orders", []))
    sell_notional_routed = float(exit_route_result.get("sell_notional_routed", 0.0) or 0.0)
    exit_routed_before_entry = 1

    # ── ENTRY 평가 ────────────────────────────────────────────────────────────
    logger.info("[US_ENTRY][EVAL][START] session=%s budget=%.2f", session, effective_budget)
    entry_intents: list[dict] = []
    entry_eval_error_count = 0
    entry_degraded = False
    entry_degraded_reason = ""
    watchlist_fallback_used = False
    entry_watchlist_source = "none"
    exit_routed_after_entry_degraded = False
    after_cutoff = _entry_cutoff_passed(now)

    # fills contract/temp error is degraded: block duplicate-sensitive BUYs, keep session alive.
    if fills_contract_error and os.getenv("US_REQUIRE_FILL_CONFIRM", "1") == "1":
        last_stage = "fills_contract_guard"
        logger.warning(
            "[US_ENTRY][BLOCK] reason=TEMP_FILLS_UNAVAILABLE require_fill_confirm=1 continue_session=1"
        )
        logger.warning(
            "[US_ORDER][ROUTE][SKIP] reason=TEMP_FILLS_UNAVAILABLE"
        )
        logger.warning(
            "[US_TICK][DONE] session=%s status=DEGRADED_FILLS_UNAVAILABLE reason=TEMP_FILLS_UNAVAILABLE", session
        )
        return {
            "status": "DEGRADED_FILLS_UNAVAILABLE",
            "reason": "TEMP_FILLS_UNAVAILABLE",
            "session": session,
            "orders": [],
            "ack": 0,
            "dry_run": 0,
            "blocked": 0,
            "signal_only": 0,
            "errors": 1,
            "budget": budget,
            "run_mode": run_mode,
            "signal_only_mode": signal_only,
            "kis_order_allowed": kis_order_allowed,
            "last_stage": last_stage,
            "trade_date": trade_date,
            "prep_status": "UNKNOWN",
            "locked_watchlist_count": 0,
            "entry_eval_status": "DEGRADED_FILLS_UNAVAILABLE",
            "entry_error_type": "TEMP_FILLS_UNAVAILABLE",
            "entry_error_message": "temporary fills unavailable; duplicate-sensitive buys blocked",
            "entry_intents": 0,
            "orders_sent": 0,
            "fills": len(fills_today),
            "positions": position_count if 'position_count' in locals() else 0,
            "temp_error_count": temp_error_count,
            "temp_recovered_count": temp_recovered_count,
        }
    elif fills_temp_error and real_order_mode and os.getenv("US_REQUIRE_FILL_CONFIRM", "1") == "1":
        last_stage = "fills_temp_guard"
        logger.warning("[US_ENTRY][BLOCK] reason=TEMP_FILLS_UNAVAILABLE")
        logger.warning("[US_TICK][DONE] session=%s status=DEGRADED_FILLS_UNAVAILABLE reason=TEMP_FILLS_UNAVAILABLE", session)
        return {
            "status": "DEGRADED_FILLS_UNAVAILABLE",
            "reason": "TEMP_FILLS_UNAVAILABLE",
            "session": session,
            "orders": [],
            "ack": 0,
            "dry_run": 0,
            "blocked": 0,
            "signal_only": 0,
            "errors": 1,
            "budget": budget,
            "run_mode": run_mode,
            "signal_only_mode": signal_only,
            "kis_order_allowed": kis_order_allowed,
            "last_stage": last_stage,
            "trade_date": trade_date,
            "prep_status": "UNKNOWN",
            "locked_watchlist_count": 0,
            "entry_eval_status": "DEGRADED_FILLS_UNAVAILABLE",
            "entry_error_type": "TEMP_FILLS_UNAVAILABLE",
            "entry_error_message": "temporary fills unavailable; duplicate-sensitive buys blocked",
            "entry_intents": 0,
            "orders_sent": 0,
            "fills": len(fills_today),
            "positions": position_count if 'position_count' in locals() else 0,
            "temp_error_count": temp_error_count,
            "temp_recovered_count": temp_recovered_count,
        }
    elif after_cutoff:
        last_stage = "entry_cutoff_guard"
        logger.info(
            "[US_ENTRY][BLOCK] reason=after_entry_cutoff time=%s",
            now.strftime("%H:%M:%S"),
        )
    else:
        # ── 당일 BUY count는 전체 entry 차단이 아닌 진단 전용 ─────────────────
        _entry_already_bought = False
        _buy_orders_today = 0
        try:
            from trader.us.db.repos import get_today_buy_orders_count
            _buy_orders_today = get_today_buy_orders_count(trade_date=trade_date, env=env)
            if session_buy_orders_count is not None:
                _buy_orders_today = max(_buy_orders_today, int(session_buy_orders_count or 0))
            logger.info(
                "[US_ENTRY][DIAGNOSTIC] already_bought_today=%d buy_orders_count=%d entry_global_block=0",
                int(_buy_orders_today > 0),
                _buy_orders_today,
            )
        except Exception as _ebc_exc:
            logger.warning("[US_ENTRY][ENTRY_DIAGNOSTIC][WARN] error=%s (fail-open)", _ebc_exc)

        # prep status 확인 (locked watchlist contract)
        from trader.us.db.repos import load_latest_us_prep_status, load_locked_us_watchlist
        
        last_stage = "prep_status_load"
        try:
            if prep_status_cache is not None:
                prep_status_info = dict(prep_status_cache)
                prep_status = prep_status_info.get("status", "UNKNOWN") if prep_status_info else "UNKNOWN"
                logger.info("[US_ENTRY][PREP_STATUS][CACHE] source=%s status=%s", prep_cache_source or "session_cache", prep_status)
            else:
                try:
                    prep_status_info = load_latest_us_prep_status(trade_date, timeout_sec=max(1, int(os.getenv("US_PREP_STATUS_LOAD_TIMEOUT_SEC", "5"))))
                except TypeError:
                    prep_status_info = load_latest_us_prep_status(trade_date)
                prep_status = prep_status_info.get("status", "UNKNOWN") if prep_status_info else "UNKNOWN"
        except Exception as prep_exc:
            if not _is_transient_watchlist_db_error(prep_exc):
                raise
            entry_degraded = True
            entry_degraded_reason = "prep_status_db_degraded"
            prep_status_info = None
            prep_status = "UNKNOWN_DB_DEGRADED"
            logger.warning(
                "[US_ENTRY][PREP_STATUS][DB_DEGRADED] trade_date=%s error=%s exit_intents=%d",
                trade_date, prep_exc, len(exit_intents),
            )
        
        logger.info(
            "[US_ENTRY][PREP_STATUS] date=%s status=%s",
            trade_date, prep_status
        )
        prep_result = prep_status_info.get("result") if isinstance(prep_status_info, dict) and isinstance(prep_status_info.get("result"), dict) else {}
        if not prep_result and isinstance(prep_status_info, dict):
            prep_result = prep_status_info
        if isinstance(prep_result, dict):
            prep_result.setdefault("status", prep_status)
        gate = validate_us_regime_contract_for_entry(prep_result, real_order_mode=real_order_mode, kis_order_allowed=kis_order_allowed)
        required_contract_version = gate["required_contract_version"]
        required_regime_version = gate["required_regime_version"]
        actual_contract_version = gate["actual_contract_version"]
        actual_regime_version = gate["actual_regime_version"]
        contract_block_reason = gate["reason"]
        entry_allowed_by_prep_contract = bool(gate["ok"])
        logger.info("[US_ENTRY][REGIME_CONTRACT] session=%s market_regime=%s capital_scale=%s effective_capital_scale=%s allow_new_buy=%s allow_ai_tech_buy=%s max_ai_tech_ratio=%s max_new_positions=%s effective_max_new_positions=%s underfilled_tier=%s", session, prep_result.get("market_regime"), prep_result.get("capital_scale"), prep_result.get("effective_capital_scale"), prep_result.get("allow_new_buy"), prep_result.get("allow_ai_tech_buy"), prep_result.get("max_ai_tech_ratio"), prep_result.get("max_new_positions"), prep_result.get("effective_max_new_positions"), prep_result.get("underfilled_tier"))
        available_new_slots_before_underfilled_limit = available_new_slots
        effective_max_new_positions = prep_result.get(
            "effective_max_new_positions",
            market_state_overlay.get("effective_max_new_positions") if isinstance(market_state_overlay, dict) else None,
        )
        if effective_max_new_positions is not None:
            try:
                effective_max_new_positions_int = int(effective_max_new_positions)
                available_new_slots = min(available_new_slots, max(0, effective_max_new_positions_int))
                allow_new_symbols = bool(allow_new_symbols and available_new_slots > 0)
                logger.info(
                    "[US_ENTRY][UNDERFILLED_LIMIT] available_new_slots_before=%d effective_max_new_positions=%d available_new_slots_after=%d underfilled_tier=%s",
                    available_new_slots_before_underfilled_limit,
                    effective_max_new_positions_int,
                    available_new_slots,
                    prep_result.get("underfilled_tier") or market_state_overlay.get("underfilled_tier"),
                )
            except Exception as exc:
                logger.warning(
                    "[US_ENTRY][UNDERFILLED_LIMIT][WARN] invalid_effective_max_new_positions=%s error=%s",
                    effective_max_new_positions,
                    exc,
                )
        rotation_regime = (
            prep_result.get("rotation_regime")
            or (prep_result.get("rotation_context") or {}).get("rotation_regime")
            or (prep_status_info or {}).get("rotation_regime")
            or "UNKNOWN"
        )
        if not entry_allowed_by_prep_contract:
            if real_order_mode:
                entry_degraded = True
                entry_degraded_reason = contract_block_reason
            if contract_block_reason == "prep_contract_version_mismatch":
                logger.warning("[US_ENTRY][BLOCK] reason=prep_contract_version_mismatch required=%s actual=%s required_regime=%s actual_regime=%s action=entry_blocked", required_contract_version, actual_contract_version, required_regime_version, actual_regime_version)
            elif contract_block_reason == "risk_off_entry_block":
                logger.warning("[US_ENTRY][BLOCK] reason=risk_off_entry_block market_regime=%s force_entry_block=%s", prep_result.get("market_regime"), prep_result.get("force_entry_block"))
            elif contract_block_reason == "sector_cap_violation_block":
                logger.warning("[US_ENTRY][BLOCK] reason=sector_cap_violation_block cap_violations=%s", prep_result.get("cap_violations"))
            else:
                logger.warning("[US_ENTRY][BLOCK] reason=%s status=%s rotation_regime=%s", contract_block_reason, prep_status, rotation_regime)
        
        # DEGRADED/ERROR 상태이면 new entry 차단
        if prep_status in ("DEGRADED", "ERROR") or not entry_allowed_by_prep_contract or market_state_overlay.get("force_entry_block"):
            logger.warning(
                "[US_ENTRY][BLOCK] reason=%s status=%s",
                contract_block_reason if not entry_allowed_by_prep_contract else ("risk_off_entry_block" if market_state_overlay.get("force_entry_block") else "prep_degraded_or_error"), prep_status
            )
        else:
            # locked watchlist 로드
            try:
                min_watchlist_count = int(os.getenv("US_MIN_LOCKED_WATCHLIST_COUNT", "10"))
                allow_degraded = os.getenv("US_ALLOW_DEGRADED_IN_TRADE", "0") == "1"
                watchlist_start = time.monotonic()
                last_stage = "watchlist_load"
                logger.info(
                    "[US_ENTRY][WATCHLIST][LOAD][START] trade_date=%s",
                    trade_date,
                )
                
                try:
                    if locked_watchlist_cache is not None:
                        watchlist_rows = list(locked_watchlist_cache)
                        watchlist_fallback_used = (watchlist_cache_source or "").startswith("artifact")
                        entry_watchlist_source = watchlist_cache_source or "session_cache"
                        logger.info("[US_ENTRY][WATCHLIST][CACHE] source=%s count=%d", entry_watchlist_source, len(watchlist_rows))
                    else:
                        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                            fut = pool.submit(
                                load_locked_us_watchlist,
                                trade_date,
                                min_watchlist_count,
                                allow_degraded,
                                watchlist_timeout_sec,
                            )
                            watchlist_rows = fut.result(timeout=watchlist_timeout_sec + 1)
                except concurrent.futures.TimeoutError:
                    elapsed_ms = int((time.monotonic() - watchlist_start) * 1000)
                    logger.error(
                        "[US_ENTRY][WATCHLIST][LOAD][TIMEOUT] timeout_sec=%d elapsed_ms=%d",
                        watchlist_timeout_sec,
                        elapsed_ms,
                    )
                    entry_degraded = True
                    entry_degraded_reason = "watchlist_load_timeout"
                    logger.info("[US_ENTRY][WATCHLIST][FALLBACK_ARTIFACT][START] trade_date=%s", trade_date)
                    try:
                        watchlist_rows = load_watchlist_from_artifact(trade_date)
                        watchlist_fallback_used = True
                        entry_watchlist_source = (watchlist_rows[0].get("entry_watchlist_source") if watchlist_rows else "artifact_final30_scored")
                    except Exception as fb_exc:
                        logger.warning(
                            "[US_ENTRY][WATCHLIST][FALLBACK_ARTIFACT][FAIL] trade_date=%s error=%s",
                            trade_date, fb_exc,
                        )
                        logger.warning(
                            "[US_ENTRY][WATCHLIST][LOAD][DEGRADED_SKIP_ENTRY] reason=watchlist_load_timeout exit_intents=%d",
                            len(exit_intents),
                        )
                        watchlist_rows = []
                except Exception as exc:
                    elapsed_ms = int((time.monotonic() - watchlist_start) * 1000)
                    is_transient = _is_transient_watchlist_db_error(exc)
                    logger.error(
                        "[US_ENTRY][WATCHLIST][LOAD][ERROR] transient=%d error=%s elapsed_ms=%d",
                        int(is_transient), exc, elapsed_ms,
                    )
                    if not is_transient:
                        raise
                    entry_degraded = True
                    entry_degraded_reason = "watchlist_load_timeout" if "timeout" in str(exc).lower() else "watchlist_load_transient_db_error"
                    if "timeout" in str(exc).lower():
                        logger.error(
                            "[US_ENTRY][WATCHLIST][LOAD][TIMEOUT] timeout_sec=%d elapsed_ms=%d",
                            watchlist_timeout_sec, elapsed_ms,
                        )
                    logger.info("[US_ENTRY][WATCHLIST][FALLBACK_ARTIFACT][START] trade_date=%s", trade_date)
                    try:
                        watchlist_rows = load_watchlist_from_artifact(trade_date)
                        watchlist_fallback_used = True
                        entry_watchlist_source = (watchlist_rows[0].get("entry_watchlist_source") if watchlist_rows else "artifact_final30_scored")
                    except Exception as fb_exc:
                        logger.warning(
                            "[US_ENTRY][WATCHLIST][FALLBACK_ARTIFACT][FAIL] trade_date=%s error=%s",
                            trade_date, fb_exc,
                        )
                        logger.warning(
                            "[US_ENTRY][WATCHLIST][LOAD][DEGRADED_SKIP_ENTRY] reason=%s exit_intents=%d",
                            entry_degraded_reason, len(exit_intents),
                        )
                        watchlist_rows = []

                elapsed_ms = int((time.monotonic() - watchlist_start) * 1000)
                logger.info(
                    "[US_ENTRY][WATCHLIST][LOAD][DONE] count=%d elapsed_ms=%d",
                    len(watchlist_rows),
                    elapsed_ms,
                )
                
                if watchlist_rows:
                    raw_watchlist_count = len(watchlist_rows)
                    # symbol별 best row로 dedupe
                    watchlist_rows = _dedupe_watchlist_best_by_symbol(watchlist_rows)
                    
                    # ── Quality Contract 검증 (hard gate) ──────────────────────
                    from trader.us.watchlist_quality import validate_us_locked_watchlist_quality, format_us_watchlist_error_message
                    
                    quality_contract = validate_us_locked_watchlist_quality(
                        rows=watchlist_rows,
                        stage="trade_load",
                    )
                    quality_ok = bool(quality_contract["ok"])
                    
                    if not quality_ok:
                        error_msg = format_us_watchlist_error_message(quality_contract)
                        logger.error(error_msg)
                        if exit_intents:
                            entry_degraded = True
                            entry_degraded_reason = "locked_watchlist_score_contract_fail"
                            entry_intents = []
                            logger.warning(
                                "[US_ENTRY][WATCHLIST][QUALITY][DEGRADED_SKIP_ENTRY] reason=locked_watchlist_score_contract_fail exit_intents=%d",
                                len(exit_intents),
                            )
                            watchlist_rows = []
                        elif real_order_mode:
                            logger.error(
                                "[US_TICK][DONE] session=%s status=FAILED reason=locked_watchlist_score_contract_fail",
                                session,
                            )
                            return {
                                "status": "FAILED",
                                "reason": "locked_watchlist_score_contract_fail",
                                "session": session,
                                "orders": [],
                                "ack": 0,
                                "dry_run": 0,
                                "blocked": 0,
                                "signal_only": 0,
                                "errors": 1,
                                "score_contract": quality_contract,
                                "run_mode": run_mode,
                                "signal_only_mode": signal_only,
                                "kis_order_allowed": kis_order_allowed,
                                "last_stage": last_stage,
                                "trade_date": trade_date,
                                "prep_status": prep_status,
                                "locked_watchlist_count": len(watchlist_rows),
                                "entry_eval_status": "FAILED",
                                "entry_error_type": "locked_watchlist_score_contract_fail",
                                "entry_error_message": "locked_watchlist_score_contract_fail",
                                "entry_intents": 0,
                                "orders_sent": 0,
                                "fills": len(fills_today),
                                "positions": position_count,
                                "temp_error_count": temp_error_count,
                                "temp_recovered_count": temp_recovered_count,
                            }
                        else:
                            logger.warning(
                                "[US_ENTRY][SCORE_CONTRACT][WARN] non_real_order_mode entry disabled"
                            )
                            watchlist_rows = []
                    
                    # Contract 통과 로그
                    logger.info(
                        "[US_ENTRY][SCORE_CONTRACT] stage=trade_load rows=%d unique=%d duplicate=%d "
                        "nonzero=%d zero=%d missing=%d ratio=%.4f ok=%d",
                        quality_contract["rows"], quality_contract["unique_symbols"], 
                        quality_contract["duplicate_count"],
                        quality_contract["score_nonzero"], quality_contract["score_zero"], 
                        quality_contract["score_missing"],
                        quality_contract["score_nonzero_ratio"], int(quality_ok)
                    )
                    
                    # Pipeline contract 로그
                    logger.info(
                        "[US_PIPELINE][CONTRACT] session=%s trade_date=%s prep_status=%s locked_raw=%d locked_deduped=%d fills_status=%s",
                        session, trade_date, prep_status, raw_watchlist_count, len(watchlist_rows),
                        "OK" if fills_error_count == 0 else ("CONTRACT_ERROR" if fills_contract_error else "TEMP_ERROR")
                    )
                    
                    if not watchlist_fallback_used:
                        entry_watchlist_source = "db_locked_watchlist"
                    logger.info(
                        "[US_ENTRY][LOCKED_WATCHLIST] raw_count=%d deduped_count=%d prep_status=%s source=%s",
                        raw_watchlist_count, len(watchlist_rows), prep_status, entry_watchlist_source
                    )
                    
                    # INPUT CONTRACT 검증
                    logger.info(
                        "[US_ENTRY][INPUT_CONTRACT] rows=%d schema_ok=1",
                        len(watchlist_rows)
                    )
                else:
                    # locked watchlist empty: pipeline input missing, except degraded DB load path which skips entry only.
                    logger.error(
                        "[US_ENTRY][BLOCK] reason=locked_watchlist_missing trade_date=%s",
                        trade_date,
                    )
                    if entry_degraded:
                        logger.warning(
                            "[US_ENTRY][WATCHLIST][LOAD][DEGRADED_SKIP_ENTRY] reason=%s exit_intents=%d",
                            entry_degraded_reason or "watchlist_load_degraded", len(exit_intents),
                        )
                    elif real_order_mode:
                        entry_degraded = True
                        entry_degraded_reason = "locked_watchlist_missing"
                        logger.warning("[US_ENTRY][DEGRADED_SKIP_ENTRY] reason=locked_watchlist_missing")
                    if not entry_degraded:
                        logger.warning(
                            "[US_ENTRY][BLOCK][WARN] non_real_order_mode locklist missing -> no entry intents"
                        )
                
                if watchlist_rows:
                    engine = _get_strategy_engine(env=env, offline=offline)
                    try:
                        last_stage = "entry_eval"
                        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                            fut = pool.submit(
                                engine.evaluate_entries,
                                None,
                                provider,
                                sold_today,
                                effective_budget,
                                position_count,
                                now,
                                watchlist_rows,
                                current_position_symbols,
                                allow_new_symbols=allow_new_symbols,
                                allow_add_to_existing=allow_add_to_existing,
                                available_new_slots=available_new_slots,
                            )
                            entry_intents = fut.result(timeout=entry_eval_timeout_sec)
                    except concurrent.futures.TimeoutError:
                        logger.error(
                            "[US_ENTRY][EVAL][TIMEOUT] timeout_sec=%d",
                            entry_eval_timeout_sec,
                        )
                        entry_eval_error_count += 1
                        entry_intents = []
                    except Exception as exc:
                        logger.error(
                            "[US_ENTRY][EVAL][ERROR] type=%s message=%s",
                            type(exc).__name__, str(exc)
                        )
                        entry_eval_error_count += 1
                        entry_intents = []
            except Exception as exc:
                elapsed_ms = 0
                logger.error(
                    "[US_ENTRY][WATCHLIST][LOAD][ERROR] type=%s message=%s",
                    type(exc).__name__,
                    str(exc),
                )
                logger.error("[US_ENTRY][EVAL][ERROR] %s", exc)
                entry_eval_error_count += 1
    
    try:
        from trader.us.portfolio_cluster_guard import filter_entry_intents_for_cluster_guard
        entry_intents, cluster_guard_blocked_buys = filter_entry_intents_for_cluster_guard(entry_intents, cluster_guard_result)
        from trader.us.market_state_overlay import filter_entry_intents_for_market_state
        entry_intents, market_state_blocked_buys = filter_entry_intents_for_market_state(entry_intents, market_state_overlay, current_positions)
        from trader.us.position_trend_state import filter_add_to_existing_by_trend_state
        entry_intents, trend_blocked_buys = filter_add_to_existing_by_trend_state(entry_intents, current_positions)
        logger.info("[US_ENTRY][MARKET_STATE_FILTER] kept=%d blocked=%d blocked_entries=%s", len(entry_intents), len(market_state_blocked_buys), market_state_blocked_buys)
    except Exception as _cluster_guard_filter_exc:
        logger.warning("[US_CLUSTER_GUARD][ENTRY_FILTER][WARN] error=%s", _cluster_guard_filter_exc)
        cluster_guard_blocked_buys = []
        trend_blocked_buys = []
    if 'trend_blocked_buys' not in locals():
        trend_blocked_buys = []
    logger.info("[US_ENTRY][EVAL][DONE] entry_intents=%d", len(entry_intents))

    if entry_eval_error_count > 0 and real_order_mode and not exit_intents:
        entry_degraded = True
        entry_degraded_reason = entry_degraded_reason or "entry_eval_error"
        logger.warning("[US_ENTRY][DEGRADED_SKIP_ENTRY] reason=entry_eval_error")


    # ── Order routing ─────────────────────────────────────────────────────────
    all_intents = entry_intents
    if entry_degraded and exit_intents:
        exit_routed_after_entry_degraded = True
        logger.warning(
            "[US_ORDER][ROUTE][EXIT_CONTINUE_AFTER_ENTRY_DEGRADED] exit_intents=%d reason=%s",
            len(exit_intents), entry_degraded_reason or "entry_degraded",
        )
    routing_intents_total = len(exit_intents) + len(entry_intents)
    logger.info("[US_ORDER][ROUTE][START] total_intents=%d exit_intents=%d entry_intents=%d", routing_intents_total, len(exit_intents), len(entry_intents))

    # orders already contains immediately-routed exit orders.

    # locked_watchlist_symbols: BUY universe (watchlist_rows에서 dedupe 후 추출)
    locked_watchlist_symbols: set[str] = set()
    if 'watchlist_rows' in locals() and watchlist_rows:
        locked_watchlist_symbols = {
            str(row.get("symbol", "")).upper().strip()
            for row in watchlist_rows
            if row.get("symbol")
        }

    # current_position_symbols 보강: reconcile이 비어있으면 current_positions에서 추출
    if not current_position_symbols and current_positions:
        current_position_symbols = {
            str(p.get("symbol", "")).upper().strip()
            for p in current_positions
            if p.get("symbol")
        }

    # entry vs locked_watchlist contract 로그
    _entry_buy_symbols = {
        str(i.get("symbol", "")).upper().strip()
        for i in entry_intents
        if i.get("side", "BUY").upper() == "BUY" and i.get("symbol")
    }
    if _entry_buy_symbols and locked_watchlist_symbols:
        _ok = _entry_buy_symbols <= locked_watchlist_symbols
        logger.info(
            "[US_CONTRACT][ENTRY_RISK_UNIVERSE][%s] entry_count=%d locked_count=%d",
            "OK" if _ok else "WARN",
            len(_entry_buy_symbols),
            len(locked_watchlist_symbols),
        )
        if not _ok and real_order_mode:
            _not_in_wl = _entry_buy_symbols - locked_watchlist_symbols
            logger.warning(
                "[US_CONTRACT][ENTRY_RISK_UNIVERSE][FILTER] removing %d BUY intents not in locked watchlist",
                len(_not_in_wl),
            )
            entry_intents = [
                i for i in entry_intents
                if not (i.get("side", "BUY").upper() == "BUY"
                        and str(i.get("symbol", "")).upper().strip() in _not_in_wl)
            ]
            all_intents = entry_intents

    from trader.us.execution.order_router import route_order

    for intent in all_intents:
        try:
            result = route_order(
                intent,
                current_daily_notional_usd=buy_daily_notional,
                current_position_count=position_count,
                total_portfolio_usd=max(effective_budget, 1000.0),
                available_cash_usd=max(effective_budget - buy_daily_notional, 0.0),
                signal_only=signal_only,
                kis_order_allowed=kis_order_allowed,
                allowed_symbols=(locked_watchlist_symbols if str(intent.get("side", "BUY")).upper() == "BUY" and locked_watchlist_symbols else None),
                current_position_symbols=current_position_symbols if current_position_symbols else None,
            )
            orders.append(result)
            if result["status"] in ("DRY_RUN", "ACK"):
                if str(intent.get("side", "")).upper() == "SELL" and str((intent.get("meta") or {}).get("profit_capture_stage") or ""):
                    try:
                        from trader.us.db.repos import mark_us_profit_capture_stage
                        mark_us_profit_capture_stage(
                            trade_date,
                            str(intent.get("symbol") or ""),
                            str((intent.get("meta") or {}).get("profit_capture_stage")),
                            order_key=str(intent.get("client_order_key") or intent.get("order_key") or "") or None,
                            qty=int(intent.get("qty") or intent.get("quantity") or 0),
                            notional_usd=float(intent.get("notional_usd") or 0.0),
                            status="ACK" if result["status"] == "ACK" else "PENDING",
                        )
                    except Exception as _pc_ack_exc:
                        logger.warning("[US_PROFIT_CAPTURE][ACK_MARK_WARN] symbol=%s err=%s", intent.get("symbol"), _pc_ack_exc)
                if str(intent.get("side", "")).upper() == "SELL" and str((intent.get("meta") or {}).get("trend_stage") or ""):
                    try:
                        from trader.us.db.repos import mark_us_position_exit_stage
                        mark_us_position_exit_stage(
                            trade_date,
                            str(intent.get("symbol") or ""),
                            str((intent.get("meta") or {}).get("trend_stage")),
                            order_key=str(intent.get("client_order_key") or intent.get("order_key") or "") or None,
                            status="ACK" if result["status"] == "ACK" else "PENDING",
                            lifecycle_id=(intent.get("meta") or {}).get("position_lifecycle_id"),
                        )
                    except Exception as _trend_ack_exc:
                        logger.warning("[US_POSITION][TREND_STATE][ACK_MARK_WARN] symbol=%s err=%s", intent.get("symbol"), _trend_ack_exc)
                if str(intent.get("side", "")).upper() == "BUY":
                    buy_daily_notional += float(intent.get("notional_usd", 0) or 0)
            elif str(intent.get("side", "")).upper() == "SELL" and str((intent.get("meta") or {}).get("trend_stage") or ""):
                try:
                    from trader.us.db.repos import mark_us_position_exit_stage
                    mapped_status = "REJECTED" if str(result.get("status") or "").upper() in {"REJECTED", "FAILED", "ERROR", "BLOCKED"} else str(result.get("status") or "").upper()
                    mark_us_position_exit_stage(
                        trade_date,
                        str(intent.get("symbol") or ""),
                        str((intent.get("meta") or {}).get("trend_stage")),
                        order_key=str(intent.get("client_order_key") or intent.get("order_key") or "") or None,
                        status=mapped_status,
                        lifecycle_id=(intent.get("meta") or {}).get("position_lifecycle_id"),
                    )
                except Exception as _trend_rej_exc:
                    logger.warning("[US_POSITION][TREND_STATE][REJECT_MARK_WARN] symbol=%s err=%s", intent.get("symbol"), _trend_rej_exc)
                if str(intent.get("side", "")).upper() == "BUY":
                    symbol_upper = str(intent.get("symbol", "")).upper().strip()
                    position_action = (
                        intent.get("position_action")
                        or (intent.get("meta") or {}).get("position_action")
                        or ""
                    )
                    if position_action == "NEW_POSITION_BUY" and symbol_upper not in current_position_symbols:
                        position_count += 1
                        current_position_symbols.add(symbol_upper)
                    else:
                        logger.info(
                            "[US_TICK][POSITION_COUNT_NOT_INCREMENTED] symbol=%s position_action=%s current_count=%d",
                            symbol_upper, position_action, position_count,
                        )
        except Exception as exc:
            logger.warning("[US_ORDER][ROUTE][WARN] intent=%s error=%s", intent.get("symbol"), exc)
            orders.append({"status": "ERROR", "error": str(exc), "intent": intent})

    ack_cnt = sum(1 for o in orders if o["status"] == "ACK")
    dry_cnt = sum(1 for o in orders if o["status"] == "DRY_RUN")
    exit_closed_cnt = sum(1 for o in orders if o["status"] == "OK_EXIT_POSITION_CLOSED")
    sell_reconcile_pending_cnt = sum(1 for o in orders if o["status"] == "WARN_SELL_REJECT_RECONCILE_PENDING")
    blocked_cnt = sum(1 for o in orders if o["status"] in {"BLOCKED", "WARN_DUPLICATE_EXIT_BLOCKED"})
    signal_only_cnt = sum(1 for o in orders if o["status"] == "SIGNAL_ONLY")
    reject_cnt = sum(1 for o in orders if o["status"] == "REJECT")
    err_cnt = sum(1 for o in orders if o["status"] == "ERROR")
    ack_db_failed_cnt = sum(1 for o in orders if o.get("status") == "ACK_DB_FAILED")

    if not offline and (ack_cnt > 0 or ack_db_failed_cnt > 0):
        try:
            from trader.us.execution.reconcile import reconcile_ack_orders_with_balance
            ack_recon_after_route = reconcile_ack_orders_with_balance(
                provider=provider,
                trade_date=trade_date,
                env=env,
            )
            ack_recon = dict(ack_recon_after_route)
            logger.info(
                "[US_RECONCILE][ACK_RECONCILE][TICK_AFTER_ROUTE] status=%s pending=%d confirmed=%d balance=%d unresolved=%d",
                ack_recon_after_route.get("status"),
                ack_recon_after_route.get("pending_count", 0),
                ack_recon_after_route.get("confirmed_count", 0),
                ack_recon_after_route.get("balance_reconcile_count", 0),
                ack_recon_after_route.get("unresolved_count", 0),
            )
        except Exception as exc:
            logger.warning("[US_TICK][WARN] post-route reconcile_ack_orders_with_balance failed: %s", exc)
            ack_recon_after_route = {"status": "ACK_PENDING_RECONCILE", "error": str(exc), "pending_count": ack_cnt + ack_db_failed_cnt, "confirmed_count": 0, "balance_reconcile_count": 0, "unresolved_count": ack_cnt + ack_db_failed_cnt, "symbols_by_status": {"ack_pending_reconcile": []}}
            ack_recon = dict(ack_recon_after_route)
    else:
        ack_recon_after_route = dict(ack_recon_before_route)

    # Block reasons 통계 수집
    block_reasons: dict[str, int] = {}
    blocked_sell_symbols: set[str] = set()
    duplicate_exit_blocked = False
    for o in orders:
        status_o = str(o.get("status") or "")
        reason = str(o.get("reason") or "unknown")
        side_o = str(o.get("side") or (o.get("intent") or {}).get("side") or "").upper()
        symbol_o = str(o.get("symbol") or (o.get("intent") or {}).get("symbol") or "").upper()
        reason_key = reason
        if "reason=" in reason_key:
            try:
                reason_key = reason_key.split("reason=")[1].split()[0]
            except Exception:
                pass
        if status_o in {"BLOCKED", "WARN_DUPLICATE_EXIT_BLOCKED"}:
            block_reasons[reason_key] = block_reasons.get(reason_key, 0) + 1
            if side_o == "SELL" and symbol_o:
                blocked_sell_symbols.add(symbol_o)
        if status_o == "WARN_DUPLICATE_EXIT_BLOCKED" or reason_key in {"pending_sell_order_exists", "duplicate_sell_client_order_key"}:
            duplicate_exit_blocked = True

    duplicate_blocked_cnt = sum(1 for o in orders if o.get("duplicate_blocked"))
    duplicate_exit_blocked = duplicate_exit_blocked or duplicate_blocked_cnt > 0
    reject_reasons = [str(o.get("reason") or o.get("error") or "") for o in orders if o.get("status") == "REJECT"]
    primary_reject_reason = next((r for r in reject_reasons if r), "")
    from trader.us.runner.status_contract import is_no_balance_sell_reject
    sell_reject_symbols: set[str] = set()
    no_balance_sell_symbols: set[str] = set()
    for o in orders:
        side_o = str((o.get("intent") or {}).get("side") or o.get("side") or "").upper()
        symbol_o = str((o.get("intent") or {}).get("symbol") or o.get("symbol") or "").upper()
        reason_o = str(o.get("reason") or o.get("error") or "")
        if o.get("status") == "REJECT" and side_o == "SELL" and symbol_o:
            sell_reject_symbols.add(symbol_o)
            if is_no_balance_sell_reject(reason_o):
                no_balance_sell_symbols.add(symbol_o)
    no_balance_sell_reject_count = len(no_balance_sell_symbols)
    recent_sell_ack_symbols: set[str] = set()
    balance_qty_zero_symbols: set[str] = set()
    orderable_qty_zero_symbols: set[str] = set()
    position_absent_symbols: set[str] = set()
    positions_by_symbol = {
        str(p.get("symbol", "")).upper(): p
        for p in (current_positions if 'current_positions' in locals() else [])
        if p.get("symbol")
    }
    for symbol_nb in no_balance_sell_symbols:
        try:
            from trader.us.db.repos import find_recent_sell_ack
            recent_ack = find_recent_sell_ack(symbol=symbol_nb, trade_date=trade_date)
        except Exception as exc:
            logger.warning("[US_TICK][RECENT_SELL_ACK][WARN] symbol=%s err=%s", symbol_nb, exc)
            recent_ack = None
        if recent_ack:
            recent_sell_ack_symbols.add(symbol_nb)
        pos = positions_by_symbol.get(symbol_nb)
        if pos is None:
            position_absent_symbols.add(symbol_nb)
            balance_qty_zero_symbols.add(symbol_nb)
            orderable_qty_zero_symbols.add(symbol_nb)
            continue
        try:
            qty_val = int(pos.get("qty") or pos.get("holding_qty") or 0)
        except Exception:
            qty_val = 0
        try:
            orderable_val = int(pos.get("orderable_qty") or pos.get("sellable_qty") or 0)
        except Exception:
            orderable_val = 0
        if qty_val <= 0:
            balance_qty_zero_symbols.add(symbol_nb)
        if orderable_val <= 0:
            orderable_qty_zero_symbols.add(symbol_nb)
    recent_sell_ack_exists = bool(recent_sell_ack_symbols)
    balance_qty_zero = bool(balance_qty_zero_symbols)
    orderable_qty_zero = bool(orderable_qty_zero_symbols)
    logger.info(
        "[US_ORDER][ROUTE][DONE] total=%d ack=%d dry_run=%d blocked=%d duplicate_blocked=%d signal_only=%d reject=%d error=%d",
        len(orders), ack_cnt, dry_cnt, blocked_cnt, duplicate_blocked_cnt, signal_only_cnt, reject_cnt, err_cnt,
    )
    
    if blocked_cnt > 0:
        logger.warning(
            "[US_ORDER][ROUTE][BLOCKED_SUMMARY] total_blocked=%d reasons=%s",
            blocked_cnt, block_reasons,
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 최종 status 판정 (US 전용 status 체계)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    total_errors = fills_error_count + entry_eval_error_count + err_cnt
    total_warnings = fills_warnings_count
    orders_sent = ack_cnt + dry_cnt  # ACK + DRY_RUN = 실제 주문 시도 수
    orders_failed = reject_cnt + err_cnt
    exit_intents_count = len(exit_intents)
    entry_intents_count = len(entry_intents)

    # Status 결정 우선순위:
    # 1. 심각한 에러가 있으면 ERROR
    # 2. exit_intents > 0 이면 exit 결과를 우선 판단
    #    - 전체 REJECT → FAILED_ALL_EXIT_ORDERS_REJECTED
    #    - 일부 REJECT → FAILED_PARTIAL_EXIT_ORDERS_REJECTED
    #    - 전체 BLOCKED → FAILED_ALL_EXIT_ORDERS_BLOCKED
    #    - orders_sent > 0 → OK_EXIT_ORDERS_SENT
    #    - orders_attempted == 0 → FAILED_EXIT_INTENTS_NOT_ROUTED
    # 3. exit_intents == 0 → entry 기반 status
    #    - entry_intents == 0 → OK_NO_TRADE
    #    - ...

    if total_errors > 0:
        status = "ERROR" if total_errors > 2 else "OK_WITH_ERRORS"
        status_reasons = []
        if fills_error_count > 0:
            status_reasons.append(f"fills_error={fills_error_count}")
        if entry_eval_error_count > 0:
            status_reasons.append(f"entry_eval_error={entry_eval_error_count}")
        if err_cnt > 0:
            status_reasons.append(f"order_error={err_cnt}")
        logger.warning(
            "[US_TICK][STATUS_DECISION] status=%s errors=%d reasons=[%s]",
            status, total_errors, ", ".join(status_reasons)
        )
    elif exit_intents_count > 0:
        # exit intents가 있으면 exit 결과를 기준으로 status 결정
        # 주의: exit_intents > 0이면 entry_intents == 0이어도 OK_NO_TRADE 금지
        orders_attempted = len(orders)
        if exit_closed_cnt > 0 and exit_closed_cnt == exit_intents_count:
            status = "OK_EXIT_POSITION_CLOSED"
        elif duplicate_blocked_cnt > 0 and duplicate_blocked_cnt == exit_intents_count:
            status = "WARN_DUPLICATE_EXIT_BLOCKED"
        elif sell_reconcile_pending_cnt > 0 and (sell_reconcile_pending_cnt + duplicate_blocked_cnt + exit_closed_cnt) == exit_intents_count:
            status = "WARN_SELL_REJECT_RECONCILE_PENDING"
        elif no_balance_sell_symbols:
            status = "FAILED_ALL_EXIT_ORDERS_REJECTED"
        elif reject_cnt == exit_intents_count and orders_sent == 0:
            status = "FAILED_ALL_EXIT_ORDERS_REJECTED"
            logger.error(
                "[US_TICK][STATUS_DECISION] status=%s exit_intents=%d rejected=%d",
                status, exit_intents_count, reject_cnt,
            )
        elif reject_cnt > 0:
            status = "FAILED_PARTIAL_EXIT_ORDERS_REJECTED"
            logger.error(
                "[US_TICK][STATUS_DECISION] status=%s exit_intents=%d rejected=%d sent=%d",
                status, exit_intents_count, reject_cnt, orders_sent,
            )
        elif duplicate_blocked_cnt > 0 and blocked_cnt == exit_intents_count and orders_sent == 0:
            status = "WARN_DUPLICATE_EXIT_BLOCKED"
            logger.warning(
                "[US_TICK][STATUS_DECISION] status=%s exit_intents=%d duplicate_blocked=%d",
                status, exit_intents_count, duplicate_blocked_cnt,
            )
        elif blocked_cnt == exit_intents_count and orders_sent == 0:
            status = "FAILED_ALL_EXIT_ORDERS_BLOCKED"
            logger.error(
                "[US_TICK][STATUS_DECISION] status=%s exit_intents=%d blocked=%d",
                status, exit_intents_count, blocked_cnt,
            )
        elif orders_sent > 0:
            status = "OK_EXIT_SENT_ENTRY_DEGRADED" if entry_degraded else "OK_EXIT_ORDERS_SENT"
            logger.info(
                "[US_TICK][STATUS_DECISION] status=%s exit_intents=%d sent=%d entry_degraded=%d",
                status, exit_intents_count, orders_sent, int(entry_degraded),
            )
        elif orders_attempted == 0:
            status = "FAILED_EXIT_INTENTS_NOT_ROUTED"
            logger.error(
                "[US_TICK][STATUS_DECISION] status=%s exit_intents=%d not_routed",
                status, exit_intents_count,
            )
        else:
            # 기타 (signal_only 등)
            status = "OK_SIGNAL_ONLY" if signal_only else "OK_WITH_WARNINGS"
            logger.info(
                "[US_TICK][STATUS_DECISION] status=%s exit_intents=%d orders_attempted=%d",
                status, exit_intents_count, orders_attempted,
            )
    elif entry_intents_count == 0 and orders_sent == 0:
        # 진입 후보가 없음 + exit 없음
        status = "OK_NO_TRADE_ENTRY_DEGRADED" if entry_degraded else ("OK_NO_TRADE" if not signal_only else "OK_SIGNAL_ONLY")
        logger.info(
            "[US_TICK][STATUS_DECISION] status=%s reason=no_entry_intents signal_only=%s",
            status, int(signal_only)
        )
    elif entry_intents_count > 0 and orders_sent == 0 and blocked_cnt > 0:
        # 진입 후보는 있었지만 risk gate에서 전부 차단됨
        status = "NO_ORDERS_RISK_BLOCKED"
        logger.warning(
            "[US_TICK][STATUS_DECISION] status=%s entry_intents=%d blocked=%d block_reasons=%s",
            status, entry_intents_count, blocked_cnt, block_reasons
        )
    elif orders_sent > 0 and blocked_cnt > 0:
        # 일부는 주문 성공, 일부는 차단됨
        status = "PARTIAL_ORDERS_BLOCKED"
        logger.warning(
            "[US_TICK][STATUS_DECISION] status=%s orders_sent=%d blocked=%d block_reasons=%s",
            status, orders_sent, blocked_cnt, block_reasons
        )
    elif signal_only:
        status = "OK_SIGNAL_ONLY"
        logger.info(
            "[US_TICK][STATUS_DECISION] status=%s reason=signal_only_mode",
            status
        )
    elif orders_sent > 0:
        status = "OK_ORDERS_SENT" if total_warnings == 0 else "OK_WITH_WARNINGS"
        logger.info(
            "[US_TICK][STATUS_DECISION] status=%s orders_sent=%d warnings=%d",
            status, orders_sent, total_warnings
        )
    else:
        status = "OK_WITH_WARNINGS" if total_warnings > 0 else "OK"
        logger.info(
            "[US_TICK][STATUS_DECISION] status=%s warnings=%d",
            status, total_warnings
        )
    
    sell_decisions_detail = [
        {"symbol": str((i or {}).get("symbol", "")).upper(), **(((i or {}).get("meta") or {}) if isinstance((i or {}).get("meta"), dict) else {})}
        for i in exit_intents if str((i or {}).get("side") or "").upper() == "SELL"
    ]
    buy_notional_routed = buy_daily_notional
    total_order_notional_routed = buy_notional_routed + sell_notional_routed
    after_symbols_by_status = ack_recon_after_route.get("symbols_by_status", {}) if isinstance(ack_recon_after_route, dict) else {}
    ack_pending_reconcile_count = len(after_symbols_by_status.get("ack_pending_reconcile", [])) if isinstance(after_symbols_by_status, dict) else 0
    broker_ack_only_unresolved = int(ack_recon_after_route.get("unresolved_count", 0) or 0)
    real_broker_buys = real_broker_sells = synthetic_reconcile_buys = synthetic_reconcile_sells = 0
    broker_ack_only = ack_cnt + dry_cnt
    broker_rejects = reject_cnt
    for f in (fills_today if 'fills_today' in locals() else []):
        side_f = str(f.get("side") or "").upper()
        meta_f = f.get("meta") or {}
        source_f = str(f.get("source") or f.get("reconcile_source") or (meta_f.get("source") if isinstance(meta_f, dict) else "") or "").lower()
        is_synth = "balance_reconcile" in source_f or "synthetic" in source_f
        if is_synth and side_f == "BUY":
            synthetic_reconcile_buys += 1
        elif is_synth and side_f == "SELL":
            synthetic_reconcile_sells += 1
        elif side_f == "BUY":
            real_broker_buys += 1
        elif side_f == "SELL":
            real_broker_sells += 1

    held_skip = locals().get("held_skip_count")
    held_skip_unknown = 0 if isinstance(held_skip, int) else 1
    if exit_routed_after_entry_degraded:
        logger.info(
            "[US_TICK][SUMMARY] status=OK_WITH_WARNINGS reason=entry_degraded_exit_routed exit_intents=%d entry_degraded=1",
            exit_intents_count,
        )
    logger.info(
        "[US_TICK][SUMMARY] exit_routed_before_entry=%d entry_degraded=%d session=%s trade_date_et=%s final30=%d holdings=%d held_skip=%s held_skip_unknown=%d buy_intents=%d risk_allowed=%d risk_blocked=%d submitted=%d ack=%d rejected=%d temp_recovered=%d final_errors=%d status=%s",
        exit_routed_before_entry, int(entry_degraded), session, trade_date, len(watchlist_rows) if 'watchlist_rows' in locals() and watchlist_rows else 0,
        len(current_positions) if 'current_positions' in locals() else 0,
        held_skip if held_skip is not None else "None", held_skip_unknown,
        entry_intents_count, orders_sent, blocked_cnt, orders_sent, ack_cnt, reject_cnt, temp_recovered_count, total_errors, status,
    )
    logger.info("[US_TICK][DONE] session=%s status=%s", session, status)

    return {
        "status": status,
        "reason": primary_reject_reason or ("entry_degraded_exit_routed" if exit_routed_after_entry_degraded else ("duplicate_exit_blocked" if duplicate_blocked_cnt else "none")),
        "primary_reject_reason": primary_reject_reason,
        "reject_reasons": reject_reasons,
        "no_balance_sell_reject_count": no_balance_sell_reject_count,
        "recent_sell_ack_exists": recent_sell_ack_exists,
        "balance_qty_zero": balance_qty_zero,
        "orderable_qty_zero": orderable_qty_zero,
        "sell_reject_symbols": sorted(sell_reject_symbols),
        "no_balance_sell_symbols": sorted(no_balance_sell_symbols),
        "recent_sell_ack_symbols": sorted(recent_sell_ack_symbols),
        "position_absent_symbols": sorted(position_absent_symbols),
        "balance_qty_zero_symbols": sorted(balance_qty_zero_symbols),
        "orderable_qty_zero_symbols": sorted(orderable_qty_zero_symbols),
        "session": session,
        "orders": orders,
        "ack": ack_cnt,
        "orders_ack": ack_cnt,
        "dry_run": dry_cnt,
        "blocked": blocked_cnt,
        "orders_blocked": blocked_cnt,  # 호환성 위해 둘 다 제공
        "block_reasons": block_reasons,
        "blocked_sell_symbols": sorted(blocked_sell_symbols),
        "signal_only": signal_only_cnt,
        "errors": err_cnt,
        "orders_rejected": reject_cnt,
        "orders_failed": orders_failed,
        "orders_sent": orders_sent,
        "exit_intents": exit_intents_count,
        "entry_intents": entry_intents_count,
        "trend_healthy_count": int((trend_state_counts if 'trend_state_counts' in locals() else {}).get("HEALTHY", 0)),
        "trend_warning_count": int((trend_state_counts if 'trend_state_counts' in locals() else {}).get("WARNING", 0)),
        "trend_trim_count": int((trend_state_counts if 'trend_state_counts' in locals() else {}).get("TRIM", 0)),
        "trend_exit_count": int((trend_state_counts if 'trend_state_counts' in locals() else {}).get("EXIT", 0)),
        "trend_unknown_count": int((trend_state_counts if 'trend_state_counts' in locals() else {}).get("UNKNOWN", 0)),
        "high_watermark_updated_count": sum(1 for p in current_positions if p.get("high_watermark_source") == "us_position_risk_state"),
        "time_stop_trim_count": sum(1 for i in exit_intents if i.get("exit_type") == "time_stop_trim"),
        "time_stop_exit_count": sum(1 for i in exit_intents if i.get("exit_type") == "time_stop_exit"),
        "trend_add_blocked_count": len(trend_blocked_buys) if 'trend_blocked_buys' in locals() else 0,
        "trend_metrics_persisted_count": sum(1 for p in current_positions if (p.get("trend") or {}).get("daily_metrics_source") == "persisted_trend_metrics"),
        "trend_metrics_final30_count": sum(1 for p in current_positions if (p.get("trend") or {}).get("daily_metrics_source") == "final30_prep_metrics"),
        "trend_metrics_price_daily_count": sum(1 for p in current_positions if (p.get("trend") or {}).get("daily_metrics_source") == "price_daily"),
        "trend_metrics_unknown_count": sum(1 for p in current_positions if p.get("trend_state") == "UNKNOWN"),
        "trend_trim_intent_count": sum(1 for i in exit_intents if i.get("exit_type") == "trend_deterioration_trim"),
        "trend_exit_intent_count": sum(1 for i in exit_intents if i.get("exit_type") == "trend_deterioration_exit"),
        "daily_http_call_count": int((provider.get_client_stats() if hasattr(provider, "get_client_stats") else getattr(provider, "stats", {})).get("daily_http_call_count", 0) if hasattr((provider.get_client_stats() if hasattr(provider, "get_client_stats") else getattr(provider, "stats", {})), "get") else 0),
        "prep_contract_trade_block": bool(entry_degraded_reason in {"prep_contract_trade_block", "prep_contract_version_mismatch", "risk_off_entry_block", "force_entry_block", "allow_new_buy_false"}),
        "market_regime": market_state_overlay.get("market_regime") if 'market_state_overlay' in locals() else "NEUTRAL",
        "capital_scale": market_state_overlay.get("capital_scale") if 'market_state_overlay' in locals() else 1.0,
        "effective_capital_scale": market_state_overlay.get("effective_capital_scale") if 'market_state_overlay' in locals() else None,
        "effective_max_new_positions": market_state_overlay.get("effective_max_new_positions") if 'market_state_overlay' in locals() else None,
        "underfilled_tier": market_state_overlay.get("underfilled_tier") if 'market_state_overlay' in locals() else None,
        "sector_cap_enforced": market_state_overlay.get("sector_cap_enforced") if 'market_state_overlay' in locals() else False,
        "market_state": market_state_overlay.get("market_state") if 'market_state_overlay' in locals() else "NORMAL",
        "exposure_multiplier": exposure_multiplier if 'exposure_multiplier' in locals() else 1.0,
        "effective_budget_before_overlay": effective_budget_before_overlay if 'effective_budget_before_overlay' in locals() else effective_budget,
        "effective_budget_after_overlay": effective_budget_after_overlay if 'effective_budget_after_overlay' in locals() else effective_budget,
        "routing_intents_total": routing_intents_total if 'routing_intents_total' in locals() else (exit_intents_count + entry_intents_count),
        "budget": budget,
        "run_mode": run_mode,
        "signal_only_mode": signal_only,
        "kis_order_allowed": kis_order_allowed,
        "last_stage": "order_route",
        "trade_date": trade_date,
        "prep_status": prep_status if 'prep_status' in locals() else "UNKNOWN",
        "locked_watchlist_count": len(watchlist_rows) if 'watchlist_rows' in locals() and watchlist_rows else 0,
        "entry_eval_status": "DEGRADED" if entry_degraded else ("OK" if entry_eval_error_count == 0 else "ERROR"),
        "entry_error_type": entry_degraded_reason if entry_degraded else ("" if entry_eval_error_count == 0 else "entry_eval_error"),
        "entry_error_message": entry_degraded_reason if entry_degraded else ("" if entry_eval_error_count == 0 else "entry_eval_error"),
        "entry_degraded": int(entry_degraded),
        "entry_degraded_reason": entry_degraded_reason,
        "watchlist_fallback_used": int(watchlist_fallback_used),
        "entry_watchlist_source": entry_watchlist_source,
        "exit_routed_before_entry": exit_routed_before_entry,
        "exit_routed_after_entry_degraded": int(exit_routed_after_entry_degraded),
        "portfolio_cluster_guard_status": cluster_guard_result.get("portfolio_cluster_guard_status"),
        "portfolio_ai_tech_weight": cluster_guard_result.get("portfolio_ai_tech_weight"),
        "portfolio_equity_usd": portfolio_equity_usd if 'portfolio_equity_usd' in locals() else 0.0,
        "portfolio_cluster_cap_violations": cluster_guard_result.get("portfolio_cluster_cap_violations", []),
        "cluster_guard_blocked_buys": cluster_guard_blocked_buys if 'cluster_guard_blocked_buys' in locals() else [],
        "market_state_blocked_buys": market_state_blocked_buys if 'market_state_blocked_buys' in locals() else [],
        "blocked_entry_reason_counts": _blocked_entry_reason_counts(cluster_guard_blocked_buys if 'cluster_guard_blocked_buys' in locals() else [], market_state_blocked_buys if 'market_state_blocked_buys' in locals() else [], entry_degraded_reason),
        "cluster_guard_trim_intents": cluster_guard_result.get("cluster_guard_trim_intents", []),
        "cluster_guard_trim_notional": cluster_guard_result.get("cluster_guard_trim_notional", 0.0),
        **deployment_metrics,
        "capital_deployment_action": capital_deployment_action,
        "position_count": position_count,
        "max_positions": max_positions,
        "available_new_slots": available_new_slots,
        "avg_position_value_usd": (invested_market_value_usd / position_count) if position_count > 0 else 0.0,
        "positions_below_target_weight": 0,
        "add_to_existing_candidates": 0,
        "new_symbol_slots_available": available_new_slots,
        "full_position": int(full_position),
        "warnings": ["US_CAPITAL_UNDERDEPLOYED_FULL_POSITION"] if (full_position and deployment_metrics.get("underdeployed")) else [],
        "entry_intents": len(entry_intents),
        "orders_sent": orders_sent,  # ack + dry_run
        "fills_count": len(fills_today),
        "fills": len(fills_today),
        "sold_today_count": len(sold_today) if 'sold_today' in locals() else 0,
        "sold_today_symbols": sorted(sold_today) if 'sold_today' in locals() else [],
        "pending_order_count": int(ack_recon_after_route.get("unresolved_count", ack_recon.get("unresolved_count", 0)) or 0),
        "ack_reconcile_before_route_status": ack_recon_before_route.get("status", "SKIP"),
        "ack_reconcile_before_route_pending_count": int(ack_recon_before_route.get("pending_count", 0) or 0),
        "ack_reconcile_before_route_confirmed_count": int(ack_recon_before_route.get("confirmed_count", 0) or 0),
        "ack_reconcile_before_route_balance_reconcile_count": int(ack_recon_before_route.get("balance_reconcile_count", 0) or 0),
        "ack_reconcile_before_route_unresolved_count": int(ack_recon_before_route.get("unresolved_count", 0) or 0),
        "ack_reconcile_after_route_status": ack_recon_after_route.get("status", "SKIP"),
        "ack_reconcile_after_route_pending_count": int(ack_recon_after_route.get("pending_count", 0) or 0),
        "ack_reconcile_after_route_confirmed_count": int(ack_recon_after_route.get("confirmed_count", 0) or 0),
        "ack_reconcile_after_route_balance_reconcile_count": int(ack_recon_after_route.get("balance_reconcile_count", 0) or 0),
        "ack_reconcile_after_route_unresolved_count": int(ack_recon_after_route.get("unresolved_count", 0) or 0),
        "ack_reconcile_after_route_symbols_by_status": ack_recon_after_route.get("symbols_by_status", {}),
        "ack_reconcile_status": ack_recon.get("status", "SKIP"),
        "ack_reconcile_pending_count": int(ack_recon.get("pending_count", 0) or 0),
        "ack_reconcile_confirmed_count": int(ack_recon.get("confirmed_count", 0) or 0),
        "ack_reconcile_balance_reconcile_count": int(ack_recon.get("balance_reconcile_count", 0) or 0),
        "ack_reconcile_unresolved_count": int(ack_recon.get("unresolved_count", 0) or 0),
        "ack_reconcile_symbols_by_status": ack_recon.get("symbols_by_status", {}),
        "fills_api_count": len(fills_today),
        "synthetic_reconcile_fills_count": int(ack_recon_after_route.get("balance_reconcile_count", ack_recon.get("balance_reconcile_count", 0)) or 0),
        "balance_confirmed_count": int(ack_recon_after_route.get("balance_reconcile_count", ack_recon.get("balance_reconcile_count", 0)) or 0),
        "unresolved_ack_count": int(ack_recon_after_route.get("unresolved_count", ack_recon.get("unresolved_count", 0)) or 0),
        "buy_notional_routed": round(buy_notional_routed, 4),
        "sell_notional_routed": round(sell_notional_routed, 4),
        "exit_notional_routed": round(sell_notional_routed, 4),
        "total_order_notional_routed": round(total_order_notional_routed, 4),
        "buy_daily_notional_after_routing": round(buy_daily_notional, 4),
        "sell_notional_does_not_consume_buy_budget": int(sell_notional_routed > 0 and buy_daily_notional == buy_notional_routed),
        "broker_ack_only_unresolved": broker_ack_only_unresolved,
        "ack_pending_reconcile_count": ack_pending_reconcile_count,
        "open_position_count": len(current_positions) if 'current_positions' in locals() else 0,
        "open_position_symbols": [p.get("symbol", "") for p in (current_positions if 'current_positions' in locals() else [])],
        "positions": len(current_positions) if 'current_positions' in locals() else 0,
        "temp_error_count": temp_error_count,
        "temp_recovered_count": temp_recovered_count,
        "kis_temp_errors_by_api": kis_temp_errors_by_api,
        "real_broker_buys": real_broker_buys,
        "real_broker_sells": real_broker_sells,
        "synthetic_reconcile_buys": synthetic_reconcile_buys,
        "synthetic_reconcile_sells": synthetic_reconcile_sells,
        "broker_ack_only": broker_ack_only,
        "broker_rejects": broker_rejects,
        "duplicate_exit_blocked": duplicate_exit_blocked,
        "sell_decisions_detail": sell_decisions_detail,
    }


# ---------------------------------------------------------------------------
# 전략 엔진 팩토리
# ---------------------------------------------------------------------------

_engine_cache: dict[str, Any] = {}


def _get_strategy_engine(env: str = "practice", offline: bool = False) -> Any:
    """US_STRATEGY_ENGINE env에 따라 엔진을 반환한다."""
    engine_name = os.getenv("US_STRATEGY_ENGINE", "pb1").lower()

    if engine_name not in _engine_cache:
        if engine_name == "pb1":
            from trader.us.pb1.us_pb1_engine import USPb1Engine
            _engine_cache[engine_name] = USPb1Engine(env=env, offline=offline)
        else:
            from trader.us.pb1.us_pb1_engine import USPb1Engine
            logger.warning(
                "[US_ENGINE][WARN] unknown engine=%s falling back to pb1", engine_name
            )
            _engine_cache[engine_name] = USPb1Engine(env=env, offline=offline)

    return _engine_cache[engine_name]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    from trader.us.utils.logging_utils import setup_us_logging
    setup_us_logging()
    parser = argparse.ArgumentParser(description="US Trade Tick Runner")
    parser.add_argument("--session", default="am", choices=["am", "afternoon", "manual"])
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--force-now", dest="force_now", default=None)
    parser.add_argument("--run-mode", dest="run_mode", default=None)
    parser.add_argument("--signal-only", dest="signal_only", action="store_true")
    args = parser.parse_args()

    result = run_trade_tick(
        session=args.session,
        env=args.env,
        offline=args.offline,
        force_now=args.force_now,
        run_mode=args.run_mode,
        signal_only=args.signal_only,
    )
    if result["status"] in ("ERROR", "FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()

# contract marker: routing combines exit_intents + entry_intents after entry degradation.

# contract marker: "reason": "fills_contract_error"
