from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List

from trader.config import (
    DIAGNOSTIC_DUMP_DIR,
    DIAGNOSTIC_FORCE_RUN,
    DIAGNOSTIC_MAX_SYMBOLS,
    KST,
)
from trader.state_store import upsert_position
from trader.time_utils import is_trading_day, is_trading_window

logger = logging.getLogger(__name__)


def _now_iso(ts: str | None = None) -> str:
    if ts:
        return ts
    return datetime.now(KST).isoformat()


def _normalize_symbol(sym: str | None) -> str:
    return str(sym or "").strip().lstrip("A").zfill(6)


def run_diagnostics(
    *,
    selected_by_market: Dict[str, Any],
    kis_client: Any,
    now_ts: str | None = None,
    pos_state: Dict[str, Any] | None = None,
    symbols: List[str] | None = None,
) -> Dict[str, Any]:
    ts = _now_iso(now_ts)
    now_dt = datetime.fromisoformat(ts)
    market_open = is_trading_day(now_dt) and is_trading_window(now_dt)
    if not is_trading_day(now_dt) and not DIAGNOSTIC_FORCE_RUN:
        logger.info(
            "[DIAG][START] ts=%s market_open=%s symbols=%d", ts, market_open, 0
        )
        logger.info("[DIAG][END] ts=%s elapsed_ms=%d", ts, 0)
        return {"status": "skipped", "ts": ts}

    pos_state = pos_state or state_store.load_state()
    holdings = kis_client.get_positions() if kis_client else []

    if symbols is None:
        selected_symbols: List[str] = []
        for rows in (selected_by_market or {}).values():
            for row in rows or []:
                code = _normalize_symbol(row.get("code") or row.get("stock_code"))
                if code and code != "000000":
                    selected_symbols.append(code)
        holding_symbols = [
            _normalize_symbol(row.get("pdno") or row.get("code")) for row in holdings or []
        ]
        symbols = sorted({s for s in selected_symbols + holding_symbols if s})
    else:
        symbols = sorted({s for s in symbols if s})
    if len(symbols) > DIAGNOSTIC_MAX_SYMBOLS:
        symbols = symbols[:DIAGNOSTIC_MAX_SYMBOLS]

    logger.info("[DIAG][START] ts=%s market_open=%s symbols=%d", ts, market_open, len(symbols))

    recon = reconcile_orphan_unknown(holdings=holdings or [], pos_state=pos_state)
    data_health = compute_data_health(symbols=symbols, kis_client=kis_client, pos_state=pos_state)
    setup = compute_setup_reasons(
        symbols=symbols,
        pos_state=pos_state,
        market_snapshot=selected_by_market or {},
    )
    exit_probe = exit_check_collapse_report(holdings=holdings or [], pos_state=pos_state)

    summary = {
        "ts": ts,
        "market_open": market_open,
        "symbols": len(symbols),
        "reconcile": recon,
        "data_health": data_health,
        "setup": setup,
        "exit_probe": exit_probe,
    }

    dump_path = DIAGNOSTIC_DUMP_DIR / f"diag_{now_dt.strftime('%Y%m%d_%H%M%S')}.json"
    try:
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dump_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        size = dump_path.stat().st_size
        logger.info("[DIAG][DUMP] path=%s bytes=%d", dump_path, size)
    except Exception as e:
        logger.warning("[DIAG][DUMP_FAIL] path=%s err=%s", dump_path, e)

    elapsed_ms = int((datetime.now(KST) - now_dt).total_seconds() * 1000)
    logger.info("[DIAG][END] ts=%s elapsed_ms=%d", ts, elapsed_ms)
    return summary


def reconcile_orphan_unknown(*, holdings: List[dict], pos_state: Dict[str, Any]) -> Dict[str, Any]:
    positions = pos_state.setdefault("positions", {})
    memory = pos_state.setdefault("memory", {})
    last_strategy_map = memory.setdefault("last_strategy_id", {})
    orphan_symbols: List[str] = []
    unknown_symbols: List[str] = []
    by_symbol: Dict[str, Any] = {}

    for row in holdings or []:
        sym = _normalize_symbol(row.get("pdno") or row.get("code"))
        if not sym:
            continue
        actual_qty = int(row.get("qty") or row.get("hldg_qty") or row.get("ord_psbl_qty") or 0)
        pos = upsert_position(pos_state, sym)
        mapped_qty = int(pos.get("qty") or 0)
        orphan_qty = max(actual_qty - mapped_qty, 0)
        strategy_id = pos.get("strategy_id")
        reason = "HAS_STRATEGY_ID"
        unknown = False
        if strategy_id:
            pass
        elif sym in last_strategy_map:
            strategy_id = last_strategy_map.get(sym)
            reason = "MEMORY_RESTORED"
        else:
            strategy_id = None
            unknown = True
            reason = "NO_STRATEGY_ID"
            unknown_symbols.append(sym)
        pos["strategy_id"] = strategy_id
        pos.setdefault("strategy_name", None)
        pos["reconcile"] = {
            "ts": _now_iso(),
            "actual_qty": actual_qty,
            "mapped_qty": mapped_qty,
            "orphan_qty": orphan_qty,
            "unknown": unknown,
            "reason": reason,
        }
        if orphan_qty > 0:
            orphan_symbols.append(sym)
            logger.info(
                "[DIAG][ORPHAN] symbol=%s actual=%d mapped=%d orphan=%d reason=%s",
                sym,
                actual_qty,
                mapped_qty,
                orphan_qty,
                reason,
            )
        if unknown:
            logger.info("[DIAG][UNKNOWN] symbol=%s qty=%d reason=%s", sym, actual_qty, reason)
        by_symbol[sym] = pos["reconcile"]

    logger.info(
        "[DIAG][RECONCILE] symbols=%d orphan=%d unknown=%d",
        len(by_symbol),
        len(orphan_symbols),
        len(unknown_symbols),
    )
    return {
        "symbols": len(by_symbol),
        "orphan_symbols": orphan_symbols,
        "unknown_symbols": unknown_symbols,
        "by_symbol": by_symbol,
    }


def compute_data_health(*, symbols: List[str], kis_client: Any, pos_state: Dict[str, Any]) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for sym in symbols:
        reasons: List[str] = []
        daily_n = 0
        prev_close = None
        intraday_n = 0
        vwap = None
        try:
            daily = kis_client.safe_get_daily_candles(sym) if kis_client else []
            daily_n = len(daily)
            if daily_n < 21:
                reasons.append(f"DAILY_TOO_SHORT({daily_n})")
            prev_close = kis_client.safe_get_prev_close(sym) if kis_client else None
            if prev_close is None:
                reasons.append("PREV_CLOSE_MISSING")
            intraday = kis_client.safe_get_intraday_bars(sym) if kis_client else []
            intraday_n = len(intraday)
            if intraday_n <= 0:
                reasons.append("INTRADAY_EMPTY")
            vwap = kis_client.safe_compute_vwap(intraday) if kis_client else None
            if vwap is None:
                reasons.append("VWAP_MISSING")
        except Exception as e:
            reasons.append("FETCH_ERROR")
            logger.warning("[DIAG][FETCH] symbol=%s kind=%s error=%s", sym, "unknown", e)

        ok = len(reasons) == 0
        payload = {
            "ts": _now_iso(),
            "daily_n": int(daily_n),
            "prev_close": prev_close,
            "intraday_n": int(intraday_n),
            "vwap": vwap,
            "ok": ok,
            "reasons": reasons,
        }
        pos = upsert_position(pos_state, sym)
        pos["data_health"] = payload
        results[sym] = payload
        logger.info(
            "[DIAG][DATA-HEALTH] symbol=%s ok=%s daily_n=%d prev_close=%s intraday_n=%d vwap=%s reasons=%s",
            sym,
            ok,
            int(daily_n),
            str(prev_close),
            int(intraday_n),
            str(vwap),
            reasons,
        )
    return results


def compute_setup_reasons(
    *,
    symbols: List[str],
    pos_state: Dict[str, Any],
    market_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    selected_set = set()
    for rows in (market_snapshot or {}).values():
        for row in rows or []:
            code = _normalize_symbol(row.get("code") or row.get("stock_code"))
            if code:
                selected_set.add(code)

    for sym in symbols:
        pos = upsert_position(pos_state, sym)
        data_health = pos.get("data_health") or {}
        reasons: List[str] = []
        missing: List[str] = []
        setup_ok = True

        if data_health and not data_health.get("ok", False):
            setup_ok = False
            reasons.append("DATA_HEALTH_DEGRADED")
            for r in data_health.get("reasons") or []:
                if r not in reasons:
                    reasons.append(r)

        if selected_set and sym not in selected_set:
            setup_ok = False
            reasons.append("NOT_IN_SELECTION")

        if setup_ok is False and not reasons:
            reasons.append("NO_SETUP_REASON")

        payload = {
            "ts": _now_iso(),
            "setup_ok": setup_ok,
            "reasons": reasons if setup_ok or reasons else ["NO_SETUP_REASON"],
            "missing": missing,
        }
        pos["setup"] = payload
        results[sym] = payload
        logger.info(
            "[DIAG][SETUP] symbol=%s setup_ok=%s reasons=%s missing=%s",
            sym,
            setup_ok,
            payload["reasons"],
            missing,
        )
    return results


def exit_check_collapse_report(*, holdings: List[dict], pos_state: Dict[str, Any]) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for row in holdings or []:
        sym = _normalize_symbol(row.get("pdno") or row.get("code"))
        if not sym:
            continue
        pos = upsert_position(pos_state, sym)
        reasons: List[str] = []
        strategy_id = pos.get("strategy_id")
        if not strategy_id:
            reasons.append("STRATEGY_ID_MISSING")
        if isinstance(pos.get("strategies"), dict) and len(pos.get("strategies", {})) > 1:
            reasons.append("MULTI_STRATEGY_NOT_TRACKED")
        reconcile_meta = pos.get("reconcile") or {}
        if int(reconcile_meta.get("orphan_qty") or 0) > 0:
            reasons.append("ORPHAN_QTY_PRESENT")
        collapsed = bool(reasons)
        payload = {
            "ts": _now_iso(),
            "strategy_id": strategy_id,
            "collapsed": collapsed,
            "reasons": reasons,
        }
        pos["exit_check_probe"] = payload
        results[sym] = payload
        logger.info(
            "[DIAG][EXIT-PROBE] symbol=%s collapsed=%s reasons=%s strategy_id=%s",
            sym,
            collapsed,
            reasons,
            strategy_id,
        )
    return results
