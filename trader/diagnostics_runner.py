from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from rolling_k_auto_trade_api.best_k_meta_strategy import run_rebalance
from trader import state_store as runtime_state_store
from trader.config import (
    DIAG_ENABLED,
    DIAGNOSTIC_DUMP_DIR,
    DIAGNOSTIC_MAX_SYMBOLS,
    DIAGNOSTIC_TARGET_MARKETS,
    KST,
)
from trader.core_utils import get_rebalance_anchor_date
from trader.data_health import check_data_health
from trader.kis_wrapper import KisAPI
from trader.setup_eval import evaluate_setup
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)


def _normalize_code(sym: str | None) -> str:
    return str(sym or "").strip().lstrip("A").zfill(6)


def _iso_now() -> str:
    return datetime.now(KST).isoformat()


def _ensure_runtime_keys(runtime_state: Dict[str, Any]) -> None:
    runtime_state.setdefault("diagnostics", {})
    runtime_state.setdefault("memory", {})
    runtime_state["diagnostics"].setdefault("orphans", {})
    runtime_state["memory"].setdefault("data_health", {})
    runtime_state["memory"].setdefault("setup_eval", {})
    runtime_state["memory"].setdefault("exit_eval", {})
    runtime_state["diagnostics"].setdefault("last_run", {})


def _filter_markets(
    selected_by_market: Dict[str, Any] | None, allowed_markets: Iterable[str] | None
) -> Dict[str, Any]:
    if not allowed_markets:
        return selected_by_market or {}
    markets = {m.strip().upper() for m in allowed_markets if m and m.strip()}
    if not markets:
        return selected_by_market or {}
    return {k: v for k, v in (selected_by_market or {}).items() if k.upper() in markets}


def _safe_qty(row: Dict[str, Any]) -> int:
    for key in ("qty", "hldg_qty", "ord_psbl_qty"):
        try:
            qty = int(float(row.get(key) or 0))
            if qty > 0:
                return qty
        except Exception:
            continue
    return 0


def _safe_avg(row: Dict[str, Any]) -> float:
    for key in ("avg_price", "pchs_avg_pric", "pchs_avg_price"):
        try:
            return float(row.get(key) or 0.0)
        except Exception:
            continue
    return 0.0


def _collect_selected_symbols(selected_by_market: Dict[str, Any] | None) -> set[str]:
    symbols: set[str] = set()
    for rows in (selected_by_market or {}).values():
        for row in rows or []:
            code = _normalize_code(
                row.get("code") or row.get("stock_code") or row.get("pdno")
            )
            if code and code != "000000":
                symbols.add(code)
    return symbols


def _collect_target_symbols(
    *,
    holdings: List[Dict[str, Any]],
    runtime_state: Dict[str, Any],
    selected_by_market: Dict[str, Any] | None,
) -> list[str]:
    symbols: set[str] = set()
    for row in holdings or []:
        code = _normalize_code(row.get("code") or row.get("pdno"))
        if code and code != "000000" and _safe_qty(row) > 0:
            symbols.add(code)
    positions = runtime_state.get("positions") or {}
    if isinstance(positions, dict):
        symbols.update(positions.keys())
    symbols.update(_collect_selected_symbols(selected_by_market))
    symbols_list = sorted(symbols)
    if DIAGNOSTIC_MAX_SYMBOLS and len(symbols_list) > DIAGNOSTIC_MAX_SYMBOLS:
        return symbols_list[:DIAGNOSTIC_MAX_SYMBOLS]
    return symbols_list


def _record_orphan_or_unknown(
    *,
    runtime_state: Dict[str, Any],
    code: str,
    qty: int,
    avg: float,
    kind: str,
    reason: str,
    ts: str,
) -> None:
    runtime_state["diagnostics"]["orphans"][code] = {
        "ts": ts,
        "qty": qty,
        "avg": avg,
        "kind": kind,
        "reason": reason,
    }
    if kind == "ORPHAN":
        logger.warning("[ORPHAN] code=%s qty=%s avg=%s reason=%s", code, qty, avg, reason)
    else:
        logger.warning("[UNKNOWN] code=%s qty=%s avg=%s reason=%s", code, qty, avg, reason)


def _guard_reasons(
    *, code: str, setup_ok: bool, reasons: List[str]
) -> List[str]:
    if setup_ok and not reasons:
        injected = ["OK"]
        logger.warning(
            "[SETUP-REASON-GUARD] code=%s setup_ok=%s reasons_was_empty -> injected=%s",
            code,
            setup_ok,
            injected,
        )
        return injected
    if (not setup_ok) and not reasons:
        injected = ["EMPTY_SETUP_REASON_GUARD"]
        logger.warning(
            "[SETUP-REASON-GUARD] code=%s setup_ok=%s reasons_was_empty -> injected=%s",
            code,
            setup_ok,
            injected,
        )
        return injected
    return reasons


def _dump_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _load_balance(kis: Optional[KisAPI]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if kis is None:
        return {}, []
    try:
        balance = kis.get_balance()
        positions = balance.get("positions") or []
        return balance, positions
    except Exception as e:
        logger.exception("[DIAG][BALANCE] failed to fetch: %s", e)
        return {}, []


def run_diagnostics(
    *,
    kis: Optional[KisAPI],
    runtime_state: Dict[str, Any],
    selected_by_market: Dict[str, Any] | None,
) -> Dict[str, Any]:
    runtime_state = runtime_state or runtime_state_store.load_state()
    _ensure_runtime_keys(runtime_state)
    ts = now_kst()
    ts_iso = ts.isoformat()

    if selected_by_market is None:
        try:
            rebalance_payload = run_rebalance(str(get_rebalance_anchor_date()), return_by_market=True)
            selected_by_market = rebalance_payload.get("selected_by_market") or {}
        except Exception as e:
            logger.exception("[DIAG][REBALANCE] failed: %s", e)
            selected_by_market = {}

    target_markets = [m for m in (DIAGNOSTIC_TARGET_MARKETS or "").split(",") if m.strip()]
    selected_by_market = _filter_markets(selected_by_market, target_markets)
    balance, holdings = _load_balance(kis)
    positions = runtime_state.get("positions") or {}

    targets = _collect_target_symbols(
        holdings=holdings,
        runtime_state=runtime_state,
        selected_by_market=selected_by_market,
    )
    runtime_state["diagnostics"]["last_run"].update({"ts": ts_iso, "targets": targets})

    orphan_n = 0
    unknown_n = 0
    for row in holdings:
        qty = _safe_qty(row)
        if qty <= 0:
            continue
        code = _normalize_code(row.get("code") or row.get("pdno"))
        avg = _safe_avg(row)
        pos = positions.get(code) if isinstance(positions, dict) else None
        if not pos:
            orphan_n += 1
            _record_orphan_or_unknown(
                runtime_state=runtime_state,
                code=code,
                qty=qty,
                avg=avg,
                kind="ORPHAN",
                reason="MISSING_IN_STATE",
                ts=ts_iso,
            )
            continue
        sid = pos.get("sid") or pos.get("strategy_id")
        if sid is None or str(sid).strip() == "" or str(sid).upper() == "UNKNOWN":
            unknown_n += 1
            _record_orphan_or_unknown(
                runtime_state=runtime_state,
                code=code,
                qty=qty,
                avg=avg,
                kind="UNKNOWN",
                reason="STRATEGY_ID_UNKNOWN",
                ts=ts_iso,
            )

    data_health_results: Dict[str, Any] = {}
    setup_eval_results: Dict[str, Any] = {}

    for code in targets:
        health = check_data_health(code, kis)
        health["ts"] = health.get("ts") or _iso_now()
        reasons = health.get("reasons") or []
        ok = bool(health.get("ok"))
        if ok and not reasons:
            reasons = ["OK"]
        if (not ok) and not reasons:
            reasons = ["UNKNOWN_DATA_HEALTH_FAIL"]
        health["reasons"] = reasons
        data_health_results[code] = health
        runtime_state["memory"]["data_health"][code] = health
        logger.info(
            "[DATA-HEALTH] code=%s ok=%s reasons=%s daily_len=%s intraday_len=%s prev_close=%s vwap=%s",
            code,
            health.get("ok"),
            health.get("reasons"),
            health.get("daily_len"),
            health.get("intraday_len"),
            health.get("prev_close"),
            health.get("vwap"),
        )

        setup = evaluate_setup(code, kis, health, runtime_state)
        setup["ts"] = setup.get("ts") or _iso_now()
        setup_ok = bool(setup.get("setup_ok"))
        setup_reasons = _guard_reasons(
            code=code, setup_ok=setup_ok, reasons=setup.get("reasons") or []
        )
        setup["reasons"] = setup_reasons
        setup_eval_results[code] = setup
        runtime_state["memory"]["setup_eval"][code] = setup
        if setup_ok:
            logger.info(
                "[SETUP-OK] %s | reasons=%s | daily=%s intra=%s",
                code,
                setup_reasons,
                setup.get("daily", {}),
                setup.get("intra", {}),
            )
        else:
            logger.info(
                "[SETUP-BAD] %s | missing=%s reasons=%s | daily=%s intra=%s",
                code,
                setup.get("missing"),
                setup_reasons,
                setup.get("daily", {}),
                setup.get("intra", {}),
            )

    exit_eval_results: Dict[str, Any] = {}
    for code, pos in (positions or {}).items():
        qty = int(pos.get("qty") or 0)
        sid = pos.get("sid") or pos.get("strategy_id") or "UNKNOWN"
        strategy_id = pos.get("strategy_id") or "UNKNOWN"
        reasons: List[str] = []
        if sid in (None, "", "UNKNOWN"):
            reasons.append("MISSING_SID")
        if strategy_id in (None, "", "UNKNOWN"):
            reasons.append("MISSING_STRATEGY_ID")
        if qty <= 0:
            reasons.append("QTY_ZERO")
        if sid in (None, "", "UNKNOWN") or strategy_id in (None, "", "UNKNOWN"):
            unknown_n += 1
            _record_orphan_or_unknown(
                runtime_state=runtime_state,
                code=code,
                qty=qty,
                avg=float(pos.get("avg_price") or 0.0),
                kind="UNKNOWN",
                reason="EXIT_STRATEGY_ID_MISSING",
                ts=ts_iso,
            )
        if not reasons:
            exit_ok = True
            reasons = ["EMPTY_EXIT_REASON_GUARD"]
        else:
            exit_ok = False
        exit_eval_results[code] = {
            "ts": _iso_now(),
            "sid": sid or "UNKNOWN",
            "strategy_id": strategy_id,
            "qty": qty,
            "exit_ok": exit_ok,
            "reasons": reasons,
        }
        runtime_state["memory"]["exit_eval"][code] = exit_eval_results[code]
        logger.info(
            "[EXIT-CHECK] code=%s sid=%s strategy_id=%s qty=%s exit_ok=%s reasons=%s",
            code,
            sid,
            strategy_id,
            qty,
            exit_ok,
            reasons,
        )

    data_health_fail_n = sum(1 for v in data_health_results.values() if not v.get("ok"))
    setup_bad_n = sum(1 for v in setup_eval_results.values() if not v.get("setup_ok"))

    diag_payload = {
        "as_of": ts_iso,
        "diag_enabled": bool(DIAG_ENABLED),
        "targets": targets,
        "orphans_n": orphan_n,
        "unknown_n": unknown_n,
        "data_health_fail_n": data_health_fail_n,
        "setup_bad_n": setup_bad_n,
        "data_health": data_health_results,
        "setup_eval": setup_eval_results,
        "exit_eval": exit_eval_results,
        "orphans": runtime_state["diagnostics"]["orphans"],
        "selected_by_market": selected_by_market or {},
        "balance": balance,
    }

    diag_path = DIAGNOSTIC_DUMP_DIR / "diag_latest.json"
    timestamped_path = DIAGNOSTIC_DUMP_DIR / f"diag_{ts.strftime('%Y%m%d_%H%M%S')}.json"
    try:
        _dump_json(diag_path, diag_payload)
        _dump_json(timestamped_path, diag_payload)
        logger.info(
            "[DIAG][DUMP] wrote=%s targets=%d orphans=%d unknown=%d",
            diag_path,
            len(targets),
            orphan_n,
            unknown_n,
        )
    except Exception as e:
        logger.exception("[DIAG][DUMP_FAIL] path=%s err=%s", diag_path, e)

    try:
        runtime_state_store.save_state(runtime_state)
    except Exception:
        logger.exception("[DIAG][STATE] failed to persist diagnostic annotations")

    return diag_payload


def run_diagnostics_once(selected_by_market: Dict[str, Any] | None = None) -> Dict[str, Any]:
    runtime_state = runtime_state_store.load_state()
    kis: KisAPI | None = None
    try:
        kis = KisAPI()
    except Exception as e:
        logger.exception("[DIAG][INIT] failed to init KisAPI: %s", e)
    return run_diagnostics(
        kis=kis,
        runtime_state=runtime_state,
        selected_by_market=selected_by_market,
    )
