from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable

from rolling_k_auto_trade_api.best_k_meta_strategy import run_rebalance
from strategy.market_data import build_market_data
from trader import state_store as runtime_state_store
from trader.config import (
    DIAG_ENABLED,
    DIAGNOSTIC_DUMP_DIR,
    DIAGNOSTIC_MAX_SYMBOLS,
    DIAGNOSTIC_TARGET_MARKETS,
)
from trader.core_utils import get_rebalance_anchor_date
from trader.data_health import check_data_health
from trader.kis_wrapper import KisAPI
from trader.setup_eval import evaluate_setup
from trader.time_utils import is_trading_day, now_kst

logger = logging.getLogger(__name__)


def _normalize_code(sym: str | None) -> str:
    return str(sym or "").strip().lstrip("A").zfill(6)


def _filter_markets(
    selected_by_market: Dict[str, Any] | None, allowed_markets: Iterable[str] | None
) -> Dict[str, Any]:
    if not allowed_markets:
        return selected_by_market or {}
    markets = {m.strip().upper() for m in allowed_markets if m and m.strip()}
    if not markets:
        return selected_by_market or {}
    return {k: v for k, v in (selected_by_market or {}).items() if k.upper() in markets}


def _collect_target_symbols(
    selected_by_market: Dict[str, Any] | None, runtime_state: Dict[str, Any]
) -> list[str]:
    symbols: set[str] = set()
    for rows in (selected_by_market or {}).values():
        for row in rows or []:
            code = _normalize_code(row.get("code") or row.get("stock_code") or row.get("pdno"))
            if code and code != "000000":
                symbols.add(code)
    positions = runtime_state.get("positions") or {}
    if isinstance(positions, dict):
        symbols.update(positions.keys())
    lots = runtime_state.get("lots") or []
    if isinstance(lots, list):
        for lot in lots:
            code = _normalize_code(lot.get("code") or lot.get("pdno"))
            if code and code != "000000":
                symbols.add(code)
    symbols_list = sorted(symbols)
    if DIAGNOSTIC_MAX_SYMBOLS and len(symbols_list) > DIAGNOSTIC_MAX_SYMBOLS:
        return symbols_list[:DIAGNOSTIC_MAX_SYMBOLS]
    return symbols_list


def _dump_payload(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    size = path.stat().st_size
    logger.info("[DIAG][DUMP] path=%s bytes=%d", path, size)


def run_diagnostics_once() -> Dict[str, Any]:
    now = now_kst()
    trading_day = is_trading_day(now)
    runtime_state = runtime_state_store.load_state()

    kis: KisAPI | None = None
    try:
        kis = KisAPI()
    except Exception as e:
        logger.exception("[DIAG][INIT] failed to init KisAPI: %s", e)

    try:
        balance = kis.get_balance() if kis else {}
        runtime_state = runtime_state_store.reconcile_with_kis_balance(runtime_state, balance)
        runtime_state_store.save_state(runtime_state)
        logger.info("[DIAG][STATE] reconciled positions=%d", len(runtime_state.get("positions", {})))
    except Exception as e:
        logger.exception("[DIAG][STATE] reconcile failed: %s", e)

    try:
        rebalance_payload = run_rebalance(str(get_rebalance_anchor_date()), return_by_market=True)
        selected_by_market = rebalance_payload.get("selected_by_market") or {}
    except Exception as e:
        logger.exception("[DIAG][REBALANCE] failed: %s", e)
        selected_by_market = {}

    target_markets = [m for m in (DIAGNOSTIC_TARGET_MARKETS or "").split(",") if m.strip()]
    selected_by_market = _filter_markets(selected_by_market, target_markets)
    logger.info(
        "[DIAG][REBALANCE] kospi=%d kosdaq=%d",
        len(selected_by_market.get("KOSPI", []) or []),
        len(selected_by_market.get("KOSDAQ", []) or []),
    )

    market_data = build_market_data(selected_by_market, kis_client=kis)
    logger.info(
        "[DIAG][MD] symbols=%d as_of=%s",
        len(market_data.get("prices", {})),
        market_data.get("as_of"),
    )

    symbols = _collect_target_symbols(selected_by_market, runtime_state)
    data_health: Dict[str, Any] = {}
    setup_result: Dict[str, Any] = {}
    for code in symbols:
        health = check_data_health(code, kis)
        data_health[code] = health
        logger.info(
            "[DATA-HEALTH] code=%s ok=%s daily_n=%s prev_close=%s intraday_n=%s vwap=%s reasons=%s",
            code,
            health.get("ok"),
            health.get("daily_n"),
            health.get("prev_close"),
            health.get("intraday_n"),
            health.get("vwap"),
            health.get("reasons"),
        )
        setup = evaluate_setup(code, kis, health, runtime_state)
        setup_result[code] = setup
        if setup.get("setup_ok"):
            logger.info("[SETUP-OK] code=%s reasons=%s", code, setup.get("reasons"))
        else:
            logger.info(
                "[SETUP-BAD] %s | missing=%s reasons=%s | daily=%s intra=%s",
                code,
                setup.get("missing"),
                setup.get("reasons"),
                setup.get("daily", {}),
                setup.get("intra", {}),
            )
        try:
            pos = runtime_state_store.upsert_position(runtime_state, code)
            pos["data_health"] = health
            pos["setup"] = setup
        except Exception:
            logger.exception("[DIAG][STATE] failed to attach diagnostics for %s", code)

    exit_checks: list[Dict[str, Any]] = []
    for lot in runtime_state.get("lots") or []:
        if str(lot.get("status") or "OPEN").upper() != "OPEN":
            continue
        lot_code = _normalize_code(lot.get("code") or lot.get("pdno"))
        record = {
            "lot_id": lot.get("lot_id"),
            "code": lot_code,
            "sid": lot.get("sid") or lot.get("strategy_id"),
            "strategy": lot.get("strategy") or "unknown",
            "qty": int(lot.get("remaining_qty") or lot.get("qty") or 0),
            "decision": "SKIP_DIAG",
            "reasons": [],
        }
        exit_checks.append(record)
        logger.info(
            "[EXIT-CHECK] lot_id=%s code=%s sid=%s strategy=%s qty=%s decision=%s reasons=%s",
            record["lot_id"],
            lot_code,
            record["sid"],
            record["strategy"],
            record["qty"],
            record["decision"],
            record["reasons"],
        )

    orphan_symbols: list[str] = []
    unknown_symbols: list[str] = []
    for code, pos in (runtime_state.get("positions") or {}).items():
        qty = int(pos.get("qty") or 0)
        if qty <= 0:
            continue
        strategy_id = pos.get("strategy_id")
        if strategy_id is None or strategy_id == "":
            orphan_symbols.append(code)
        elif str(strategy_id).upper() == "UNKNOWN":
            unknown_symbols.append(code)
    if orphan_symbols:
        logger.warning("[ORPHAN] n=%d symbols=%s", len(orphan_symbols), orphan_symbols[:20])
    if unknown_symbols:
        logger.warning("[UNKNOWN] n=%d symbols=%s", len(unknown_symbols), unknown_symbols[:20])

    payload = {
        "ts": now.isoformat(),
        "diag_enabled": bool(DIAG_ENABLED),
        "trading_day": trading_day,
        "selected_by_market": selected_by_market,
        "data_health": data_health,
        "setup": setup_result,
        "orphans": orphan_symbols,
        "unknowns": unknown_symbols,
        "exit_checks": exit_checks,
        "positions": runtime_state.get("positions", {}),
    }

    try:
        runtime_state_store.save_state(runtime_state)
    except Exception:
        logger.exception("[DIAG][STATE] failed to persist diagnostic annotations")

    dump_path = DIAGNOSTIC_DUMP_DIR / f"diag_{now.strftime('%Y%m%d_%H%M%S')}.json"
    try:
        _dump_payload(dump_path, payload)
    except Exception as e:
        logger.warning("[DIAG][DUMP_FAIL] path=%s err=%s", dump_path, e)

    return payload
