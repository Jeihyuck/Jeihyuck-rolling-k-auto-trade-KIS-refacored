from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from pathlib import Path

from trader.config import MARKET_MAP
from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo, ReconcileLogRepo
from trader.reconcile_db import evaluate_stale_db_guard
from trader.kis_wrapper import KisAPI, KisTemporaryError
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)


def _first_value(row: dict, keys: list[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return value
    return None


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


def _parse_date_time(row: dict) -> datetime:
    date_raw = _first_value(row, ["ord_dt", "trd_dt", "ccld_dt", "ord_date", "date"])
    time_raw = _first_value(row, ["ord_tmd", "trd_tmd", "ccld_tmd", "ord_time", "time"])
    if date_raw:
        date_raw = str(date_raw).replace("-", "")
    if time_raw:
        time_raw = str(time_raw).replace(":", "")
    if date_raw and time_raw and len(date_raw) == 8 and len(time_raw) >= 4:
        try:
            return datetime.strptime(f"{date_raw}{time_raw[:6]}", "%Y%m%d%H%M%S")
        except Exception:
            pass
    if date_raw and len(date_raw) == 8:
        try:
            return datetime.strptime(date_raw, "%Y%m%d")
        except Exception:
            pass
    return now_kst()


def _parse_side(row: dict) -> str:
    raw = str(_first_value(row, ["sll_buy_dvsn_cd", "sll_buy_dvsn", "buy_sell_gb", "side"]) or "").strip()
    if raw in {"01", "1", "SELL", "S"} or "매도" in raw.upper():
        return "SELL"
    if raw in {"02", "2", "BUY", "B"} or "매수" in raw.upper():
        return "BUY"
    return "UNKNOWN"


def _normalize_code(value: Any) -> str:
    return str(value or "").strip().zfill(6)


def reconcile_today(*, engine, kis: KisAPI, env: str, run_id: str | None, strategy: str) -> dict[str, object]:
    today = now_kst().strftime("%Y%m%d")
    degraded_reason: str | None = None
    try:
        resp = kis.inquire_daily_ccld(start_date=today, end_date=today)
    except KisTemporaryError as exc:
        logger.warning("[RECONCILE][DEGRADED] temporary error: %s", exc)
        degraded_reason = "temporary"
        resp = {"output1": [], "output2": []}
    if not isinstance(resp, dict):
        degraded_reason = degraded_reason or "invalid_response"
        resp = {"output1": [], "output2": []}
    rows = resp.get("output1") or resp.get("output2") or resp.get("output") or []
    if isinstance(rows, dict):
        rows = [rows]
    orders_repo = OrdersRepo(engine)
    fills_repo = FillsRepo(engine)
    ledger_repo = LedgerEventsRepo(engine)
    reconcile_repo = ReconcileLogRepo(engine)

    order_count = 0
    fill_count = 0
    for row in rows or []:
        code = _normalize_code(_first_value(row, ["pdno", "stck_shrn_iscd", "code"]))
        if not code:
            continue
        side = _parse_side(row)
        qty = _to_int(_first_value(row, ["ord_qty", "qty", "tot_ccld_qty", "ord_qty_sum"])) or 0
        price = _to_float(_first_value(row, ["ord_unpr", "ord_price", "avg_prvs", "ccld_prc"])) or 0.0
        kis_odno = str(_first_value(row, ["odno", "ODNO", "ordno"]) or "").strip() or None
        status = str(_first_value(row, ["ord_stat_cd", "ord_stat", "status"]) or "RECONCILED").strip().upper()
        market = MARKET_MAP.get(code) or str(_first_value(row, ["excg_dvsn_cd", "market"]) or "").strip() or None
        order_time = _parse_date_time(row)
        client_order_key = f"{env}:{strategy}:{today}:{code}:{side}:{kis_odno or 'reconcile'}"

        orders_repo.upsert_reconciled_order(
            env=env,
            run_id=run_id,
            strategy=strategy,
            sid=1,
            mode=1,
            code=code,
            market=market,
            side=side,
            ord_type=str(_first_value(row, ["ord_dvsn_cd", "ord_type"]) or "RECONCILED"),
            qty=qty,
            limit_price=price,
            stage="RECONCILE",
            client_order_key=client_order_key,
            kis_odno=kis_odno,
            status=status,
            request_json=row,
            response_json=row,
            submitted_at=order_time,
            acked_at=order_time,
        )
        order_count += 1

        filled_qty = _to_int(_first_value(row, ["ccld_qty", "tot_ccld_qty", "filled_qty"]))
        filled_price = _to_float(_first_value(row, ["ccld_prc", "avg_prvs", "filled_price"]))
        if filled_qty and filled_price is not None and side != "UNKNOWN":
            fills_repo.upsert_fill(
                env=env,
                run_id=run_id,
                order_id=None,
                kis_odno=kis_odno,
                trade_id=str(_first_value(row, ["ccld_no", "trade_id", "exec_id"]) or "") or None,
                code=code,
                market=market,
                side=side,
                qty=filled_qty,
                price=filled_price,
                fee=0.0,
                tax=0.0,
                filled_at=order_time,
                raw_json=row,
            )
            fill_count += 1

    reasons = [f"orders:{order_count}", f"fills:{fill_count}"]
    if degraded_reason:
        reasons.append(f"degraded:{degraded_reason}")
    ledger_repo.append_event(
        env=env,
        run_id=run_id,
        strategy=strategy,
        event_type="RECONCILE",
        ts=now_kst(),
        ok=True,
        reasons=reasons,
        payload_json={"orders": order_count, "fills": fill_count, "degraded": degraded_reason},
    )
    logger.info("[RECONCILE][DONE] env=%s orders=%s fills=%s", env, order_count, fill_count)
    reconcile_repo.append_log(
        env=env,
        strategy=strategy,
        tick_ts=now_kst(),
        action="reconcile_today",
        details_json={"orders": order_count, "fills": fill_count, "degraded": degraded_reason},
    )
    return {"ok": True, "orders": order_count, "fills": fill_count, "degraded": degraded_reason}


def reconcile_kis(
    *,
    engine,
    kis: KisAPI,
    env: str,
    run_id: str | None,
    strategy: str,
    tick_ts: datetime,
    balance_snapshot: dict | None = None,
    bot_state_dir: str | None = None,
) -> dict[str, object]:
    holdings_error = None
    holdings_rows: list[dict] = []
    try:
        snapshot = balance_snapshot or kis.get_balance_cached(force=True)
        holdings_rows = snapshot.get("output1") or []
    except KisTemporaryError as exc:
        holdings_error = str(exc)
        logger.error("[KIS][HTTP][FAIL_SOFT] step=holdings err=%s", exc, exc_info=True)
        holdings_rows = []
    except Exception as exc:
        holdings_error = str(exc)
        logger.error("[KIS][HTTP][FAIL_SOFT] step=holdings err=%s", exc, exc_info=True)
        holdings_rows = []

    try:
        reconcile_result = reconcile_today(engine=engine, kis=kis, env=env, run_id=run_id, strategy=strategy)
    except Exception as exc:
        logger.error("[KIS][HTTP][FAIL_SOFT] step=reconcile_today err=%s", exc, exc_info=True)
        reconcile_result = {"ok": False, "reason": "reconcile_today_failed", "err": str(exc)}
    orders_count = int(reconcile_result.get("orders") or 0)
    fills_count = int(reconcile_result.get("fills") or 0)

    positions_repo = PositionsRepo(engine)
    restored = positions_repo.restore_missing_from_holdings(
        env=env,
        strategy=strategy,
        sid=1,
        mode=1,
        holdings=holdings_rows,
    )
    if restored:
        logger.warning("[RECONCILE][POSITIONS][RESTORE] env=%s restored=%s", env, restored)

    guard_result = None
    guard_reason = None
    allow_purge = None
    if bot_state_dir:
        allow_purge, guard_reason, guard_result = evaluate_stale_db_guard(
            bot_state_dir=Path(bot_state_dir),
            tick_ts=tick_ts,
            kis_holdings_empty=len(holdings_rows) == 0,
            orders_count=orders_count,
            fills_count=fills_count,
            had_kis_error=holdings_error is not None,
        )
        if holdings_error:
            allow_purge = False
            guard_reason = guard_reason or "holdings_error"
            logger.warning(
                "[RECONCILE][STALE_DB_GUARD] allow_purge=0 reason=holdings_error err=%s",
                holdings_error,
            )
        if allow_purge:
            logger.warning(
                "[RECONCILE][STALE_DB_GUARD] allow_purge=1 empty_streak=%s",
                guard_result.get("empty_streak") if isinstance(guard_result, dict) else None,
            )
        else:
            logger.info(
                "[RECONCILE][STALE_DB_GUARD] allow_purge=0 reason=%s empty_streak=%s",
                guard_reason,
                guard_result.get("empty_streak") if isinstance(guard_result, dict) else None,
            )

    reconcile_repo = ReconcileLogRepo(engine)
    reconcile_repo.append_log(
        env=env,
        strategy=strategy,
        tick_ts=tick_ts,
        action="reconcile_kis",
        details_json={
            "orders": orders_count,
            "fills": fills_count,
            "holdings": len(holdings_rows),
            "restored_positions": restored,
            "holdings_error": holdings_error,
            "guard_reason": guard_reason,
            "allow_purge": allow_purge,
        },
    )
    reconcile_result.update(
        {
            "holdings": len(holdings_rows),
            "restored_positions": restored,
            "holdings_error": holdings_error,
            "guard_reason": guard_reason,
            "allow_purge": allow_purge,
        }
    )
    return reconcile_result
