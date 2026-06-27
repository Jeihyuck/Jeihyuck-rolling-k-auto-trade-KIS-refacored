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


def _is_transient_watchlist_db_error(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(pattern in text for pattern in _TRANSIENT_WATCHLIST_DB_ERROR_PATTERNS)


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
    fills_contract_error = False
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
        grace = int(os.getenv("US_OPEN_RECHECK_GRACE_SEC", "10"))
        if 0 <= seconds_to_open <= grace:
            logger.info(
                "[US_MARKET_PHASE][WAIT_UNTIL_OPEN] session=%s seconds_to_open=%d grace=%d",
                session, seconds_to_open, grace,
            )
            if not force_now:
                time.sleep(seconds_to_open + 1)
                now = now_ny()
                phase = market_phase(now)
            else:
                logger.info("[US_MARKET_PHASE][FORCE_NOW_NO_SLEEP]")
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
    tick_timeout_sec = int(os.getenv("US_TICK_TIMEOUT_SEC", "90"))
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
    logger.info("[US_RECONCILE][START] session=%s", session)
    try:
        from trader.us.execution.reconcile import reconcile_positions
        recon = reconcile_positions(provider=provider)
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
    position_count = len(current_positions)

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

    # fills contract error 발생 시 신규 BUY 차단 및 즉시 ERROR 반환
    if fills_contract_error and os.getenv("US_REQUIRE_FILL_CONFIRM", "1") == "1":
        last_stage = "fills_contract_guard"
        logger.error(
            "[US_ENTRY][BLOCK] reason=fills_contract_error require_fill_confirm=1"
        )
        logger.error(
            "[US_ORDER][ROUTE][SKIP] reason=fills_contract_error"
        )
        logger.error(
            "[US_TICK][DONE] session=%s status=ERROR reason=fills_contract_error", session
        )
        return {
            "status": "FAILED",
            "reason": "fills_contract_error",
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
            "entry_error_type": "fills_contract_error",
            "entry_error_message": "fills_contract_error",
            "entry_intents": 0,
            "orders_sent": 0,
            "fills": len(fills_today),
            "positions": position_count if 'position_count' in locals() else 0,
            "temp_error_count": temp_error_count,
            "temp_recovered_count": temp_recovered_count,
        }
    elif fills_temp_error and real_order_mode and os.getenv("US_REQUIRE_FILL_CONFIRM", "1") == "1":
        last_stage = "fills_temp_guard"
        logger.error("[US_ENTRY][BLOCK] reason=fills_temp_error_real_order")
        logger.error("[US_TICK][DONE] session=%s status=FAILED reason=fills_temp_error", session)
        return {
            "status": "FAILED",
            "reason": "fills_temp_error",
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
            "entry_error_type": "fills_temp_error",
            "entry_error_message": "fills_temp_error",
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
        
        # DEGRADED/ERROR 상태이면 new entry 차단
        if prep_status in ("DEGRADED", "ERROR"):
            logger.warning(
                "[US_ENTRY][BLOCK] reason=prep_degraded_or_error status=%s",
                prep_status
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
    logger.info("[US_ORDER][ROUTE][START] total_intents=%d", len(all_intents))

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
                if str(intent.get("side", "")).upper() == "BUY":
                    buy_daily_notional += float(intent.get("notional_usd", 0) or 0)
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
