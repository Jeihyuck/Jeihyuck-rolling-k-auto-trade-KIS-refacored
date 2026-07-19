#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    (ROOT / path).write_text(text, encoding="utf-8")


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one literal match, found {count}: {old[:120]!r}")
    write(path, text.replace(old, new, 1))


def replace_regex(path: str, pattern: str, new: str) -> None:
    text = read(path)
    updated, count = re.subn(pattern, new, text, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError(f"{path}: expected one regex match, found {count}: {pattern[:120]!r}")
    write(path, updated)


# Explicit trade dates and no TypeError compatibility retries in live paths.
replace_once(
    "trader/us/execution/order_router.py",
    '''    def _persist_with_trade_date(func, payload):
        try:
            return func(payload, trade_date=trade_date)
        except TypeError:
            # Compatibility for older injected test adapters; production repo
            # functions accept and receive the explicit US trade_date above.
            return func(payload)
''',
    '''    def _persist_with_trade_date(func, payload):
        return func(payload, trade_date=trade_date)
''',
)
replace_once(
    "trader/us/execution/order_router.py",
    "positions = load_us_positions_by_symbols([symbol]) if symbol else {}",
    "positions = load_us_positions_by_symbols([symbol], as_of=trade_date) if symbol else {}",
)
replace_once(
    "trader/us/execution/order_router.py",
    "db_positions = load_us_positions_by_symbols([symbol]) if symbol else {}",
    "db_positions = load_us_positions_by_symbols([symbol], as_of=trade_date) if symbol else {}",
)

new_reconcile_block = '''    logger.info("[US_RECONCILE][START] session=%s tick_index=%s should_reconcile_balance=%d interval=%s", session, tick_index, int(should_reconcile_balance), balance_reconcile_interval)
    try:
        if should_reconcile_balance:
            from trader.us.execution.reconcile import reconcile_positions
            recon = reconcile_positions(provider=provider, trade_date=trade_date)
        else:
            from trader.us.db.repos import load_positions as _load_positions_for_reconcile_skip
            _positions = _load_positions_for_reconcile_skip(trade_date)
            recon = {
                "status": "SKIPPED_BALANCE_RECONCILE",
                "reason": "reconcile_interval_skip",
                "positions": _positions,
                "position_count": len(_positions),
                "position_symbols": [str(p.get("symbol") or "").upper() for p in _positions if p.get("symbol")],
                "block_new_entry": False,
                "authoritative_positions": False,
                "preserve_previous_positions": True,
            }
        logger.info(
            "[US_RECONCILE][DONE] status=%s positions=%s",
            recon.get("status"),
            recon.get("position_count", 0),
        )
    except TypeError as exc:
        logger.error("[US_RECONCILE][CONTRACT_ERROR] %s", exc)
        recon = {
            "status": "CONTRACT_ERROR",
            "reason": "reconcile_internal_type_error",
            "error": str(exc),
            "block_new_entry": True,
            "authoritative_positions": False,
            "preserve_previous_positions": True,
            "positions": [],
            "position_count": 0,
        }
    except Exception as exc:
        logger.error("[US_RECONCILE][ERROR] %s", exc)
        recon = {
            "status": "ERROR",
            "reason": "reconcile_failed",
            "error": str(exc),
            "block_new_entry": True,
            "authoritative_positions": False,
            "preserve_previous_positions": True,
            "positions": [],
            "position_count": 0,
        }
    
    # reconcile CONTRACT_ERROR'''
replace_regex(
    "trader/us/runner/trade_tick_runner.py",
    r'    logger\.info\("\[US_RECONCILE\]\[START\].*?    # reconcile CONTRACT_ERROR',
    new_reconcile_block,
)

new_fill_save = '''    # fills DB 저장
    if fills_today:
        try:
            from trader.us.db.repos import save_fills_with_result
            fill_save_result = save_fills_with_result(fills_today, trade_date=trade_date)
        except Exception as exc:
            logger.error("[US_TICK][FILL_SAVE_ERROR] %s", exc)
            fill_save_result = {"status": "DB_ERROR", "error": str(exc)}
        if fill_save_result.get("status") != "OK":
            logger.error("[US_TICK][FAILED] reason=fill_persistence_failed result=%s", fill_save_result)
            return {
                "status": "FAILED",
                "reason": "fill_persistence_failed",
                "session": session,
                "orders": [],
                "ack": 0,
                "dry_run": 0,
                "blocked": 0,
                "signal_only": 0,
                "errors": 1,
                "trade_date": trade_date,
                "fills": len(fills_today),
                "positions": int(recon.get("position_count") or 0),
                "fill_save_result": fill_save_result,
                "block_new_entry": True,
            }
        try:
            _mark_trend_stages_from_records(fills_today, trade_date=trade_date, status="FILLED")
        except Exception as exc:
            logger.warning("[US_POSITION][TREND_STAGE][FILL_MARK_WARN] err=%s", exc)

    # ACK reconcile'''
replace_regex(
    "trader/us/runner/trade_tick_runner.py",
    r"    # fills DB 저장\n    if fills_today:.*?    # ACK reconcile",
    new_fill_save,
)

new_position_section = '''    # reconcile 결과 positions DB 저장
    recon_positions = recon.get("positions", [])
    authoritative_recon = bool(
        recon.get("status") == "OK"
        and recon.get("balance_fetch_status") == "OK"
        and recon.get("authoritative_positions") is True
        and recon.get("preserve_previous_positions") is False
    )
    if authoritative_recon:
        try:
            save_position_snapshot(
                recon_positions,
                trade_date=trade_date,
                balance_fetch_status="OK",
                balance_parse_status="OK",
                authoritative_positions=True,
                preserve_previous_positions=False,
                close_source="kis_tick_balance",
            )
        except Exception as exc:
            logger.error("[US_TICK][POSITION_PERSIST_ERROR] %s", exc)
            return {
                "status": "FAILED",
                "reason": "authoritative_position_persist_failed",
                "session": session,
                "orders": [],
                "errors": 1,
                "trade_date": trade_date,
                "block_new_entry": True,
            }
    elif recon.get("preserve_previous_positions"):
        logger.warning("[US_RECONCILE][SKIP_ZERO_SNAPSHOT] reason=balance_fetch_failed preserve_previous=1")

    # reconcile log DB 저장
    try:
        payload = {
            "status": recon.get("status", "OK"),
            "message": recon.get("error", ""),
            "position_count": len(recon_positions),
            "total_pvs": recon.get("total_pvs_usd", 0),
            "detail": {"session": session},
        }
        save_reconcile_log(payload, trade_date=trade_date)
    except Exception as exc:
        logger.warning("[US_TICK][WARN] save_reconcile_log failed: %s", exc)

    # ── 현재 포지션 ───────────────────────────────────────────────────────────
    from trader.us.db.repos import load_positions as db_load_positions
    if authoritative_recon:
        current_positions = list(recon_positions)
    else:
        try:
            current_positions = db_load_positions(trade_date)
        except Exception:
            current_positions = []
    try:
        from trader.us.position_lifecycle_state import reconcile_us_position_lifecycles'''
replace_regex(
    "trader/us/runner/trade_tick_runner.py",
    r"    # reconcile 결과 positions DB 저장.*?    try:\n        from trader\.us\.position_lifecycle_state import reconcile_us_position_lifecycles",
    new_position_section,
)
